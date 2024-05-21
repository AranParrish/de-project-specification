# Creates EventBridge rules and targets to schedule Lambda function executions

# EventBridge rule to trigger the lambda function on a 20 minute schedule
resource "aws_cloudwatch_event_rule" "extract_lambda_trigger" {
    name        = "extract_lambda_trigger"
    description = "Triggers the extract_lambda function"
    schedule_expression = "rate(20 minutes)"
}

# Set the EventBridge trigger to the extract_lambda target
resource "aws_cloudwatch_event_target" "extract_target_lambda" {
    rule      = aws_cloudwatch_event_rule.extract_lambda_trigger.name
    target_id = "extract_lambda"
    arn       = aws_lambda_function.extract_lambda.arn
}

# Grant permission for the extract_lambda to be triggered by the EventBridge
resource "aws_lambda_permission" "extract_allow_cloudwatch" {
    statement_id  = "AllowExecutionFromCloudWatch"
    action        = "lambda:InvokeFunction"
    function_name = aws_lambda_function.extract_lambda.function_name
    principal     = "events.amazonaws.com"
    source_arn    = aws_cloudwatch_event_rule.extract_lambda_trigger.arn
}

# Create a Log Group for Eventbridge to push logs to
resource "aws_cloudwatch_log_group" "MyLogGroupExtract" {
  name_prefix = "/aws/events/extract/terraform"
}


# For processed data

# EventBridge rule to trigger the lambda function on a 20 minute schedule
# If there is any changes in ingestion zone bucket it should be triggered
resource "aws_cloudwatch_event_rule" "processed_lambda_trigger" {
    name        = "processed_lambda_trigger"
    description = "Triggers the processed_lambda function"
    schedule_expression = "rate(30 minutes)"
}

# Set the EventBridge trigger to the processed_lambda target
resource "aws_cloudwatch_event_target" "processed_target_lambda" {
    rule      = aws_cloudwatch_event_rule.processed_lambda_trigger.name
    target_id = "processed_lambda"
    arn       = aws_lambda_function.processed_lambda.arn
}

# Grant permission for the processed_lambda to be triggered by the EventBridge
resource "aws_lambda_permission" "processed_allow_cloudwatch" {
    statement_id  = "AllowExecutionFromCloudWatch"
    action        = "lambda:InvokeFunction"
    function_name = aws_lambda_function.processed_lambda.function_name
    principal     = "events.amazonaws.com"
    source_arn    = aws_cloudwatch_event_rule.processed_lambda_trigger.arn
}

# Create a Log Group for Eventbridge to push logs to
resource "aws_cloudwatch_log_group" "MyLogGroupProcessed" {
  name_prefix = "/aws/events/processed/terraform"
}

