resource "aws_ecr_repository" "batch_job" {
  name         = "${var.project_name}-batch-job"
  force_delete = true
}
