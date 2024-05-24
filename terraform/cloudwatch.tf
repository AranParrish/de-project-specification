# Defines IAM policies and CloudWatch log groups/streams to manage and monitor logs for Lambda functions

# Get the current account identity and region

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

# Create IAM policy for CloudWatch Logs permissions for extract lambda function
resource "aws_iam_policy" "extract_cloudwatch_logs_policy" {
  name        = "ExtractCloudWatchLogsPermissions"
  description = "IAM policy for extract lambda CloudWatch Logs permissions"

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
        Resource = "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.extract_lambda_name}:*"
      }
    ]
  })
}

# For processed data

# Create IAM policy for CloudWatch Logs permissions for processed lambda function
resource "aws_iam_policy" "processed_cloudwatch_logs_policy" {
  name        = "ProcessedCloudWatchLogsPermissions"
  description = "IAM policy for processed lambda CloudWatch Logs permissions"

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
        Resource = "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.processed_lambda_name}:*"
      }
    ]
  })
}

# For load lambda

# Create IAM policy for CloudWatch Logs permissions for load lambda function
resource "aws_iam_policy" "load_cloudwatch_logs_policy" {
  name        = "LoadCloudWatchLogsPermissions"
  description = "IAM policy for load lambda CloudWatch Logs permissions"

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
        Resource = "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.load_lambda_name}:*"
      }
    ]
  })
}