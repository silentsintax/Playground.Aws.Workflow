import os
import time
import uuid
import boto3

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["TABLE_NAME"])

def lambda_handler(event, context):
    for record in event.get("Records", []):
        line = record["body"]
        item = {
            "id": str(uuid.uuid4()),
            "line": line,
            "received_at": int(time.time()),
        }
        table.put_item(Item=item)
    return {"statusCode": 200}
