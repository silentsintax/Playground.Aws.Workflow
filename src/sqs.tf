resource "aws_sqs_queue" "lines_dlq" {
  name = "${var.project_name}-lines-dlq"
}

resource "aws_sqs_queue" "lines" {
  name                       = "${var.project_name}-lines"
  visibility_timeout_seconds = 60
  message_retention_seconds  = 86400

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.lines_dlq.arn
    maxReceiveCount     = 5
  })
}
