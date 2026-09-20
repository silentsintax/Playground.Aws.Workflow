import sys
import os
import uuid
import boto3

def main():
    if len(sys.argv) < 3:
        print("Uso: process_file.py <bucket> <key>")
        sys.exit(1)

    bucket = sys.argv[1]
    key = sys.argv[2]
    queue_url = os.environ["SQS_QUEUE_URL"]

    s3 = boto3.client("s3")
    sqs = boto3.client("sqs")

    print(f"Lendo s3://{bucket}/{key}")
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8")

    lines = [line for line in body.splitlines() if line.strip()]
    print(f"{len(lines)} linha(s) encontrada(s). Enviando para o SQS...")

    batch = []
    for line in lines:
        batch.append({"Id": str(uuid.uuid4()), "MessageBody": line})
        if len(batch) == 10:
            sqs.send_message_batch(QueueUrl=queue_url, Entries=batch)
            batch = []
    if batch:
        sqs.send_message_batch(QueueUrl=queue_url, Entries=batch)

    print("Concluído.")

if __name__ == "__main__":
    main()
