resource "aws_dynamodb_table" "lines" {
  name         = "${var.project_name}-lines"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "id"

  attribute {
    name = "id"
    type = "S"
  }
}
