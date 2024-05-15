# Create IAM policy for CloudWatch Logs permissions
resource "aws_iam_policy" "cloudwatch_logs_policy" {
  name        = "CloudWatchLogsPermissions"
  path        = "/"
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