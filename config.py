config = {
    "aws_access": {
        "aws_access_key_id": 'Your aws_access_key_id',
        "aws_secret_access_key":'Your aws_secret_access_key'
    },

    "kafka": {
        "kafka.bootstrap.servers": "Your kafka.bootstrap.servers",
        "subscribe": "stock",
        "startingOffsets": "earliest",
        "maxOffsetsPerTrigger": "10",
        "failOnDataLoss": "false"

    }
}