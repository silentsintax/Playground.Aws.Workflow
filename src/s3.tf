resource "aws_s3_bucket" "upload" {
  bucket        = "${var.project_name}-uploads-${data.aws_caller_identity.current.account_id}"
  force_destroy = true
}

# Habilita o envio de eventos do bucket para o Event Bus padrão do EventBridge
resource "aws_s3_bucket_notification" "eventbridge" {
  bucket      = aws_s3_bucket.upload.id
  eventbridge = true
}
