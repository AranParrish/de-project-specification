# Create CloudWatch Logs group
resource "aws_cloudwatch_log_group" "extract_lambda_log_group" {
  name              = "/aws/extract_lambda/logs"
  retention_in_days = 30
}

# Create CloudWatch Logs stream
resource "aws_cloudwatch_log_stream" "extract_lambda_log_stream" {
  name           = "extract-lambda-log-stream"
  log_group_name = aws_cloudwatch_log_group.application_log_group.name
}

# Set CloudWatch Logs retention policy
resource "aws_cloudwatch_log_group" "extract_lambda_log_group_retention" {
  name              = aws_cloudwatch_log_group.application_log_group.name
  retention_in_days = 30
}