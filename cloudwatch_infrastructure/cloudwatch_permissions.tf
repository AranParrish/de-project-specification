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

# Create IAM role for application
resource "aws_iam_role" "application_role" {
  name = "ApplicationRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"  # change this based on application's needs
        }
      }
    ]
  })
}

# Attach CloudWatch Logs policy to the application role
resource "aws_iam_role_policy_attachment" "application_role_cloudwatch_logs_policy" {
  policy_arn = aws_iam_policy.cloudwatch_logs_policy.arn
  role       = aws_iam_role.application_role.name
}