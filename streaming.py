from typing import Optional
import json
import fastavro
import io

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import regexp_replace, expr, col, udf, from_json, window, max, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

from config import config
from streaming_helper import write_to_dynamodb

def deserialize_avro(serialized_data: bytes) -> Optional[str]:
    # Deserialize the Avro-encoded data
    bytes_io = io.BytesIO(serialized_data)
    bytes_io.seek(0)
    records = list(fastavro.reader(bytes_io))
    # Return the first record (assuming it's a single record)
    if records:
        return json.dumps(records[0])
    else:
        return None

schema =  StructType([
    StructField("Datetime", StringType(), nullable=True),
    StructField("Open", DoubleType(), nullable=True),
    StructField("High", DoubleType(), nullable=True),
    StructField("Low", DoubleType(), nullable=True),
    StructField("Close", DoubleType(), nullable=True),
    StructField("Volume", LongType(), nullable=True),
    StructField("Quotes", StringType(), nullable=True)
])   

kafka_options = {
    "kafka.bootstrap.servers": config["kafka"]["kafka.bootstrap.servers"],
    "subscribe": config["kafka"]["subscribe"],
    "startingOffsets": config["kafka"]["startingOffsets"],
    "maxOffsetsPerTrigger": config["kafka"]["maxOffsetsPerTrigger"],
    "failOnDataLoss": config["kafka"]["failOnDataLoss"]
}

class StockStream:
    def __init__(self, kafka_options: dict, schema: StructType):
        self.kafka_options = kafka_options
        self.schema = schema

    def read_kafka(self, spark: SparkSession) -> DataFrame:
        
        print("starting reading")        
        return (
            spark.readStream.format("kafka")
            .options(**self.kafka_options)
            .load()
        )
    
    @staticmethod
    def clean_data(df: DataFrame) -> DataFrame:
        # Define the UDF
        deserialize_udf = udf(deserialize_avro,StringType())
        #deserialize and clean data
        deserialized_df = df.withColumn("deserialized_value", deserialize_udf(df["value"]))
        deserialized_df  = deserialized_df.select(from_json(col("deserialized_value"), schema).alias('deserialized_value')).select(("deserialized_value.*"))
        deserialized_df = deserialized_df.withColumn("Datetime", regexp_replace(deserialized_df["Datetime"], "T", " "))
        deserialized_df = deserialized_df.withColumn("Datetime", regexp_replace(deserialized_df["Datetime"], "Z", ""))
        return deserialized_df.withColumn("Datetime", expr("to_timestamp(Datetime, 'yyyy-MM-dd HH:mm:ss')"))
    
    
    @staticmethod
    def window_function(cleaned_df: DataFrame) -> DataFrame:
        return (cleaned_df.withWatermark("Datetime", "60 minutes")
                        .groupBy(window(col("Datetime"), "5 minutes"))
                        .agg(max("Close").alias("MaxClosePrice"))
                        .select("window.start", "window.end", "MaxClosePrice")
                        .withColumn("StartDate", date_format("start", "yyyy-MM-dd"))\
                        .selectExpr("StartDate", "concat(hour(start), ':', minute(start), ':', second(start)) as StartTime",\
                    "concat(hour(end), ':', minute(end), ':', second(end)) as EndTime", "round(MaxClosePrice, 3) as MaxClosePrice")
                )
    
    @staticmethod
    def display_result(batch_df: DataFrame, batch_id: int):
        print(f'BATCH NUMBER {batch_id}')
        if batch_df.count() == 0:
            print("Empty batch")
        else:
            write_to_dynamodb(batch_df)
            # batch_df.show(truncate=False)
            # batch_df.printSchema()

    def save_result(self, aggregated_df: DataFrame):
        print(f"\nStarting Silver Stream...", end="")
    
        query = (aggregated_df.writeStream.queryName("streaming")
                 .option("checkpointLocation", "/home/jovyan/work/checkpoints")
                 .outputMode("update")
                 .trigger(processingTime="10 seconds")
                 .foreachBatch(self.display_result)
                 .start()
        )
        
        query.awaitTermination()
        

if __name__ == "__main__":
    spark = SparkSession.builder.appName("streaming").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    stream = StockStream(kafka_options, schema)
    df = stream.read_kafka(spark)
    cleaned_df = stream.clean_data(df)
    aggregated_df = stream.window_function(cleaned_df)
    stream.save_result(aggregated_df)
