# Defines IAM policies and CloudWatch log groups/streams to manage and monitor logs for Lambda functions

# Create IAM policy for CloudWatch Logs permissions
resource "aws_iam_policy" "cloudwatch_logs_policy" {
  name        = "CloudWatchLogsPermissions"
  description = "IAM policy for CloudWatch Logs permissions"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams",
          "logs:GetLogEvents",
          "logs:FilterLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# Create CloudWatch Logs group
resource "aws_cloudwatch_log_group" "extract_lambda_log_group" {
  name              = "/aws/extract_lambda/logs"
  retention_in_days = 30
}

# Create CloudWatch Logs stream
resource "aws_cloudwatch_log_stream" "extract_lambda_log_stream" {
  name           = "extract-lambda-log-stream"
  log_group_name = aws_cloudwatch_log_group.extract_lambda_log_group.name
}


# For processed data

# Same IAM policy for CloudWatch Logs permissions

# Create CloudWatch Logs group
resource "aws_cloudwatch_log_group" "processed_lambda_log_group" {
  name              = "/aws/processed_lambda/logs"
  retention_in_days = 30
}

# Create CloudWatch Logs stream
resource "aws_cloudwatch_log_stream" "processed_lambda_log_stream" {
  name           = "processed-lambda-log-stream"
  log_group_name = aws_cloudwatch_log_group.processed_lambda_log_group.name
}