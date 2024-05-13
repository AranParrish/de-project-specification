# Create SNS topic
resource "aws_sns_topic" "application_notifications" {
  name = "application-notifications"
}

# Subscribe email endpoint to SNS topic
resource "aws_sns_topic_subscription" "email_subscription" {
  topic_arn = aws_sns_topic.application_notifications.arn
  protocol  = "email"
  endpoint  = "your-email@example.com"  # replace with relevant email address
}

# Create CloudWatch alarm for application errors
resource "aws_cloudwatch_metric_alarm" "application_error_alarm" {
  alarm_name          = "ApplicationErrorAlarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"  # adjust namespace based on application
  period              = "60"
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "Alarm when application errors occur"
  alarm_actions       = [aws_sns_topic.application_notifications.arn]
}