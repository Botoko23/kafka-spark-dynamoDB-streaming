import time
import json
from decimal import Decimal

from config import config

import boto3
from botocore.exceptions import ClientError
from pyspark.sql import DataFrame


# Define function to write to DynamoDB with backoff and retry
def write_to_dynamodb(df: DataFrame):
    # Initialize DynamoDB client
    dynamodb = boto3.resource('dynamodb',    
                            aws_access_key_id=config["aws_access"]["aws_access_key_id"],
                            aws_secret_access_key=config["aws_access"]["aws_secret_access_key"],
                            region_name='us-east-1')
    
    records = df.toJSON().collect()

    max_retries = 1
    base_delay = 5.0  # Initial delay in seconds
    attempts = 0
    
    table = dynamodb.Table('test')
    while attempts <= max_retries:
        try:
            # Write to DynamoDB
            with table.batch_writer() as batch:
                for record in records:
                    record_json = json.loads(record)
                    record_json['MaxClosePrice'] = Decimal(str(record_json['MaxClosePrice']))
                    batch.put_item(Item=record_json)
                    
            print("Batch write successful")
            break
        except ClientError as e:
            if e.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
                # Throttling occurred, and retry
                print(f"Throttling occurred, retrying in {base_delay} seconds...")
                time.sleep(base_delay)
                attempts += 1
            else:
                # Other error occurred, raise exception
                raise e
    else:
        # Exceeded maximum retries, raise exception
        raise Exception("Exceeded maximum retries, unable to write to DynamoDB")


if __name__ == "__main__":
    pass