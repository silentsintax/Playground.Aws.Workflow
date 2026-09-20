resource "aws_cloudwatch_event_rule" "s3_upload" {
  name        = "${var.project_name}-s3-upload-rule"
  description = "Dispara quando um objeto é criado no bucket de upload"

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = {
        name = [aws_s3_bucket.upload.bucket]
      }
    }
  })
}

resource "aws_cloudwatch_event_target" "batch_target" {
  rule     = aws_cloudwatch_event_rule.s3_upload.name
  arn      = aws_batch_job_queue.queue.arn
  role_arn = aws_iam_role.eventbridge_batch_role.arn

  batch_target {
    job_definition = aws_batch_job_definition.process_file.arn
    job_name       = "${var.project_name}-process-file"
  }

  # Extrai bucket/key do evento S3 e envia como Parameters do Batch job
  # (usados no command via Ref::bucket / Ref::key)
  input_transformer {
    input_paths = {
      bucket = "$.detail.bucket.name"
      key    = "$.detail.object.key"
    }
    input_template = <<TEMPLATE
{
  "bucket": <bucket>,
  "key": <key>
}
TEMPLATE
  }
}
