# Sets up SNS topics and subscriptions for notifications and alerts

# Create IAM policy for SNS permissions
resource "aws_iam_policy" "sns_policy" {
  name        = "SNSPermissions"
  description = "IAM policy for SNS permissions"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "sns:Publish",
          "sns:Subscribe",
          "sns:CreateTopic",
          "sns:GetTopicAttributes",
          "sns:SetTopicAttributes",
          "sns:DeleteTopic",
          "sns:ListSubscriptionsByTopic"
        ]
        Resource = "*"
      }
    ]
  })
}

# Create SNS topic
resource "aws_sns_topic" "extract_lambda_notifications" {
  name = "extract_lambda-notifications"
}

# Subscribe email endpoint to SNS topic
resource "aws_sns_topic_subscription" "extract_email_subscription" {
  topic_arn = aws_sns_topic.extract_lambda_notifications.arn
  protocol  = "email"
  endpoint  = "ellybalci@gmail.com"
}

# Create CloudWatch alarm for extract_lambda errors
resource "aws_cloudwatch_metric_alarm" "extract_lambda_error_alarm" {
  alarm_name          = "ExtractLambdaErrorAlarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = "Errors"
  namespace           = "AWS/extract_lambda"
  period              = "60"
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "Alarm when extract_lambda errors occur"
  alarm_actions       = [aws_sns_topic.extract_lambda_notifications.arn]
}


# For processed data

# Same IAM policy for SNS permissions

# Create SNS topic
resource "aws_sns_topic" "processed_lambda_notifications" {
  name = "processed_lambda-notifications"
}

# Subscribe email endpoint to SNS topic
resource "aws_sns_topic_subscription" "processed_email_subscription" {
  topic_arn = aws_sns_topic.processed_lambda_notifications.arn
  protocol  = "email"
  endpoint  = "ellybalci@gmail.com"
}

# Create CloudWatch alarm for processed_lambda errors
resource "aws_cloudwatch_metric_alarm" "processed_lambda_error_alarm" {
  alarm_name          = "ProcessedLambdaErrorAlarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = "Errors"
  namespace           = "AWS/processed_lambda"
  period              = "60"
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "Alarm when processed_lambda errors occur"
  alarm_actions       = [aws_sns_topic.processed_lambda_notifications.arn]
}

# For load lambda

# Same IAM policy for SNS permissions

# Create SNS topic
resource "aws_sns_topic" "load_lambda_notifications" {
  name = "load_lambda-notifications"
}

# Subscribe email endpoint to SNS topic
resource "aws_sns_topic_subscription" "load_email_subscription" {
  topic_arn = aws_sns_topic.load_lambda_notifications.arn
  protocol  = "email"
  endpoint  = "ellybalci@gmail.com"
}

# Create CloudWatch alarm for processed_lambda errors
resource "aws_cloudwatch_metric_alarm" "load_lambda_error_alarm" {
  alarm_name          = "LoadLambdaErrorAlarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = "Errors"
  namespace           = "AWS/load_lambda"
  period              = "60"
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "Alarm when load_lambda errors occur"
  alarm_actions       = [aws_sns_topic.load_lambda_notifications.arn]
}