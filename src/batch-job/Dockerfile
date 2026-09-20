FROM python:3.12-slim

RUN pip install --no-cache-dir boto3

WORKDIR /app
COPY process_file.py .

# O comando real (com bucket/key) é injetado pelo Job Definition do AWS Batch
