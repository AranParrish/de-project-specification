# Create SNS topic
resource "aws_sns_topic" "extract_lambda_notifications" {
  name = "extract_lambda-notifications"
}

# Subscribe email endpoint to SNS topic
resource "aws_sns_topic_subscription" "email_subscription" {
  topic_arn = aws_sns_topic.extract_lambda_notifications.arn
  protocol  = "email"
  endpoint  = "ellybalci@gmail.com"  # replace with relevant email address
}

# Create CloudWatch alarm for extract_lambda errors
resource "aws_cloudwatch_metric_alarm" "extract_lambda_error_alarm" {
  alarm_name          = "ExtractLambdaErrorAlarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"  # adjust namespace based on extract_lambda
  period              = "60"
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "Alarm when extract_lambda errors occur"
  alarm_actions       = [aws_sns_topic.extract_lambda_notifications.arn]
}