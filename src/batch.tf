resource "aws_security_group" "batch_sg" {
  name        = "${var.project_name}-batch-sg"
  description = "Egress liberado para o job do Batch"
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_batch_compute_environment" "fargate" {
  name = "${var.project_name}-ce"
  type                     = "MANAGED"
  service_role             = aws_iam_role.batch_service_role.arn

  compute_resources {
    type               = "FARGATE"
    max_vcpus          = 4
    subnets            = data.aws_subnets.default.ids
    security_group_ids = [aws_security_group.batch_sg.id]
  }

  depends_on = [aws_iam_role_policy_attachment.batch_service_role_attach]
}

resource "aws_batch_job_queue" "queue" {
  name     = "${var.project_name}-queue"
  state    = "ENABLED"
  priority = 1

  compute_environment_order {
    order               = 1
    compute_environment = aws_batch_compute_environment.fargate.arn
  }
}

resource "aws_batch_job_definition" "process_file" {
  name                  = "${var.project_name}-process-file"
  type                  = "container"
  platform_capabilities = ["FARGATE"]

  # bucket e key chegam via EventBridge (Ref::bucket / Ref::key)
  parameters = {
    bucket = ""
    key    = ""
  }

  container_properties = jsonencode({
    image      = "${aws_ecr_repository.batch_job.repository_url}:latest"
    command    = ["python", "process_file.py", "Ref::bucket", "Ref::key"]
    jobRoleArn = aws_iam_role.batch_job_task_role.arn
    executionRoleArn = aws_iam_role.ecs_task_execution_role.arn

    networkConfiguration = {
      assignPublicIp = "ENABLED"
    }

    fargatePlatformConfiguration = {
      platformVersion = "LATEST"
    }

    resourceRequirements = [
      { type = "VCPU", value = "1" },
      { type = "MEMORY", value = "2048" }
    ]

    environment = [
      { name = "SQS_QUEUE_URL", value = aws_sqs_queue.lines.url }
    ]
  })
}
