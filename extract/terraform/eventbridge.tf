resource "aws_cloudwatch_event_rule" "extract_lambda_trigger" {
    name        = "extract_lambda_trigger"
    description = "Triggers the Lambda function when database is altered"

    event_pattern = <<PATTERN
{
    "source": [
        "custom.myDatabase"
    ],
    "detail-type": [
        "Database Alteration"
    ]
}
PATTERN
}

# TODO: Implement a mechanism to send custom events to EventBridge when the database is altered.
# The custom event should have a source of "custom.myDatabase" and a detail type of "Database Alteration".
# This will allow the EventBridge rule to match the custom events and trigger the Lambda function.

resource "aws_cloudwatch_event_target" "target_lambda" {
    rule      = aws_cloudwatch_event_rule.extract_lambda_trigger.name
    target_id = "extract_lambda"
    arn       = aws_lambda_function.extract_lambda.arn
}

resource "aws_lambda_permission" "permission" {
    statement_id  = "AllowExecutionFromCloudWatch"
    action        = "lambda:InvokeFunction"
    function_name = aws_lambda_function.extract_lambda.function_name
    principal     = "events.amazonaws.com"
    source_arn    = aws_cloudwatch_event_rule.extract_lambda_trigger.arn
}

