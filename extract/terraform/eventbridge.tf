# Creates EventBridge rules and targets to schedule Lambda function executions

# EventBridge rule to trigger the lambda function on a 20 minute schedule
resource "aws_cloudwatch_event_rule" "extract_lambda_trigger" {
    name        = "extract_lambda_trigger"
    description = "Triggers the extract_lambda function"
    schedule_expression = "rate(20 minutes)"
}

# Set the EventBridge trigger to the extract_lambda target
resource "aws_cloudwatch_event_target" "target_lambda" {
    rule      = aws_cloudwatch_event_rule.extract_lambda_trigger.name
    target_id = "extract_lambda"
    arn       = aws_lambda_function.extract_lambda.arn
}

# Grant permission for the extract_lambda to be triggered by the EventBridge
resource "aws_lambda_permission" "allow_cloudwatch" {
    statement_id  = "AllowExecutionFromCloudWatch"
    action        = "lambda:InvokeFunction"
    function_name = aws_lambda_function.extract_lambda.function_name
    principal     = "events.amazonaws.com"
    source_arn    = aws_cloudwatch_event_rule.extract_lambda_trigger.arn
}

# Create a Log Group for Eventbridge to push logs to
resource "aws_cloudwatch_log_group" "MyLogGroup" {
  name_prefix = "/aws/events/terraform"
}

