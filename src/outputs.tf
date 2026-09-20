output "upload_bucket_name" {
  value = aws_s3_bucket.upload.bucket
}

output "ecr_repository_url" {
  value = aws_ecr_repository.batch_job.repository_url
}

output "sqs_queue_url" {
  value = aws_sqs_queue.lines.url
}

output "dynamodb_table_name" {
  value = aws_dynamodb_table.lines.name
}

output "batch_job_queue_name" {
  value = aws_batch_job_queue.queue.name
}

output "lambda_function_name" {
  value = aws_lambda_function.sqs_to_dynamo.function_name
}
