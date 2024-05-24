# Sets up SNS topics and subscriptions for notifications and alerts

resource "aws_cloudwatch_log_metric_filter" "extract_lambda_error_filter" {
  name           = "ExtractErrorFilter"
  pattern        = "\"[ERROR]\""
  log_group_name = "/aws/lambda/${var.extract_lambda_name}"

  metric_transformation {
    name      = "ExtractErrorCount"
    namespace = "ExtractErrors"
    value     = "1"
  }
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
  metric_name         = "ExtractErrorCount"
  namespace           = "ExtractErrors"
  period              = "60"
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "Alarm when extract_lambda errors occur"
  alarm_actions       = [aws_sns_topic.extract_lambda_notifications.arn]
}


# For processed data

resource "aws_cloudwatch_log_metric_filter" "transform_lambda_error_filter" {
  name           = "TransformErrorFilter"
  pattern        = "\"[ERROR]\""
  log_group_name = "/aws/lambda/${var.processed_lambda_name}"

  metric_transformation {
    name      = "TransformErrorCount"
    namespace = "TransformErrors"
    value     = "1"
  }
}

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
resource "aws_cloudwatch_metric_alarm" "transform_lambda_error_alarm" {
  alarm_name          = "ProcessedLambdaErrorAlarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = "TransformErrorCount"
  namespace           = "TransformErrors"
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