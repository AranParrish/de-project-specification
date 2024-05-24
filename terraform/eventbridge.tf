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

# For processed data

# Grants S3 permission to trigger the processed lambda function

resource "aws_lambda_permission" "allow_s3_processed_lambda_permission" {
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.processed_lambda.function_name
  principal = "s3.amazonaws.com"
  source_arn = aws_s3_bucket.ingestion_s3.arn
}

# Creates notification trigger for when an object is created in the ingestion zone S3 bucket

resource "aws_s3_bucket_notification" "ingestion_bucket_notification" {
  bucket = aws_s3_bucket.ingestion_s3.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.processed_lambda.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_s3_processed_lambda_permission]
}

# For warehouse

# Grants S3 permission to trigger the warehouse lambda function

resource "aws_lambda_permission" "allow_s3_wh_lambda_permission" {
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.load_lambda.function_name
  principal = "s3.amazonaws.com"
  source_arn = aws_s3_bucket.processed_s3.arn
}

# Creates notification trigger for when an object is created in the processed zone S3 bucket

resource "aws_s3_bucket_notification" "processed_bucket_notification" {
  bucket = aws_s3_bucket.processed_s3.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.load_lambda.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_s3_wh_lambda_permission]
}