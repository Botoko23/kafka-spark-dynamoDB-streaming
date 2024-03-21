from time import sleep
import json
import fastavro
import io

from kafka import KafkaProducer
import boto3

from data_gen import data_generator
from config import config

# Create a Glue client
glue_client = boto3.client('glue', aws_access_key_id=config["aws_access"]["aws_access_key_id"],
                        aws_secret_access_key=config["aws_access"]["aws_secret_access_key"],
                        region_name='us-east-1')

avro_schema= None

try:
    schema_message = glue_client.get_schema_version(
    SchemaId={
        'SchemaName': 'stock-schema',
        'RegistryName': 'my-registry'
    },
    SchemaVersionNumber={
        'LatestVersion': True
    }
    )
    avro_schema = json.loads(schema_message['SchemaDefinition'])

except Exception as e:
    print(f"Error: {e}")

def serializer(data: dict, avro_schema: dict) -> bytes:
    bytes_io = io.BytesIO()
    fastavro.writer(bytes_io, avro_schema, [data])
    serialized_data = bytes_io.getvalue()
    return serialized_data


# Create the producer
producer = KafkaProducer(bootstrap_servers=['54.87.149.66'], value_serializer=lambda data: serializer(data, avro_schema))

if __name__ == "__main__":
    data_gen = data_generator()
    for data in data_gen:
        record_metadata = producer.send('stock', value=data).get(timeout=5)
        print(record_metadata.topic)
        print(record_metadata.partition)
        print(record_metadata.offset)
        sleep(5)




      
