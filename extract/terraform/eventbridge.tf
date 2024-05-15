resource "aws_cloudwatch_event_rule" "extract_lambda_trigger" {
    name        = "extract_lambda_trigger"
    description = "Triggers the Lambda function when database is altered"
    schedule_expression = "rate(30 minutes)"
}
# may change name depending on "source"
resource "aws_cloudwatch_event_target" "target_lambda" {
    rule      = aws_cloudwatch_event_rule.extract_lambda_trigger.name
    target_id = "extract_lambda"
    arn       = aws_lambda_function.extract_lambda.arn
}

resource "aws_lambda_permission" "allow_cloudwatch" {
    statement_id  = "AllowExecutionFromCloudWatch"
    action        = "lambda:InvokeFunction"
    function_name = aws_lambda_function.extract_lambda.function_name
    principal     = "events.amazonaws.com"
    source_arn    = aws_cloudwatch_event_rule.extract_lambda_trigger.arn # need to add the correct arn from the lambda function
}

#================================================================================

data "aws_caller_identity" "current" {}

# Create a Log Group for Eventbridge to push logs to
resource "aws_cloudwatch_log_group" "MyLogGroup" {
  name_prefix = "/aws/events/terraform"
}

# Create a Log Policy to allow Cloudwatch to Create log streams and put logs
resource "aws_cloudwatch_log_resource_policy" "MyCloudWatchLogPolicy" {
  policy_name     = "Terraform-CloudWatchLogPolicy-${data.aws_caller_identity.current.account_id}"
  policy_document = <<POLICY
{
  "Version": "2012-10-17",
  "Id": "CWLogsPolicy",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": [ 
          "events.amazonaws.com",
          "delivery.logs.amazonaws.com"
          ]
      },
      "Action": [
        "logs:CreateLogStream",
        "logs:PutLogEvents"
        ],
      "Resource": "${aws_cloudwatch_log_group.MyLogGroup.arn}",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "${aws_cloudwatch_event_rule.MyEventRule.arn}"
        }
      }
    }
  ]
}
POLICY  
}

#Create a new Event Rule
resource "aws_cloudwatch_event_rule" "MyEventRule" {
  schedule_expression = "rate(30 minutes)" 
}

#Set the log group as a target for the Eventbridge rule
resource "aws_cloudwatch_event_target" "MyRuleTarget" {
  rule = aws_cloudwatch_event_rule.MyEventRule.name
  arn  = aws_cloudwatch_log_group.MyLogGroup.arn
}

#output the CloudWatch Log Stream Name
output "CW-Logs-Stream-Name" {
  value       = aws_cloudwatch_log_group.MyLogGroup.id
  description = "The CloudWatch Log Group Name"
}