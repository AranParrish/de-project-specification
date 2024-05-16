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

# # Create a Log Policy to allow Cloudwatch to create log streams and put logs
# resource "aws_cloudwatch_log_resource_policy" "MyCloudWatchLogPolicy" {
#   policy_name     = "EventBridge-CloudWatchLogPolicy"
#   policy_document = <<POLICY
# {
#   "Version": "2012-10-17",
#   "Id": "CWLogsPolicy",
#   "Statement": [
#     {
#       "Effect": "Allow",
#       "Principal": {
#         "Service": [ 
#           "events.amazonaws.com",
#           "delivery.logs.amazonaws.com"
#           ]
#       },
#       "Action": [
#         "logs:CreateLogStream",
#         "logs:PutLogEvents"
#         ],
#       "Resource": "${aws_cloudwatch_log_group.MyLogGroup.arn}",
#       "Condition": {
#         "ArnEquals": {
#           "aws:SourceArn": "${aws_cloudwatch_event_rule.extract_lambda_trigger.arn}"
#         }
#       }
#     }
#   ]
# }
# POLICY  
# }

# #Create a new Event Rule
# resource "aws_cloudwatch_event_rule" "MyEventRule" {
#   schedule_expression = "rate(30 minutes)" 
# }

# #Set the log group as a target for the Eventbridge rule
# resource "aws_cloudwatch_event_target" "MyRuleTarget" {
#   rule = aws_cloudwatch_event_rule.MyEventRule.name
#   arn  = aws_cloudwatch_log_group.MyLogGroup.arn
# }

# #output the CloudWatch Log Stream Name
# output "CW-Logs-Stream-Name" {
#   value       = aws_cloudwatch_log_group.MyLogGroup.id
#   description = "The CloudWatch Log Group Name"
# }