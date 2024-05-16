resource "aws_lambda_function" "extract_lambda" {
    function_name = "${var.lambda_name}" # Need to provide main lambda function name
    role = aws_iam_role.extract_lambda_role.arn
    filename=data.archive_file.extract_lambda_zip.output_path
    source_code_hash = data.archive_file.extract_lambda_zip.output_base64sha256
    layers = [aws_lambda_layer_version.python_dotenv_layer.arn]
    handler = "extract_lambda.lambda_handler"
    runtime = "python3.12"
    timeout = 10

# Add dependencies for lambda s3 access, cloudwatch access, and eventbridge access
    depends_on = [
    aws_iam_role_policy_attachment.lambda_s3_policy_attachment,
    aws_iam_role_policy_attachment.extract_lambda_cloudwatch_logs_policy
    # EventBridge dependency to be added
  ]

# Add the ingestion zone data bucket as an environment variable
  environment {
    variables = {
      ingestion_zone_bucket = resource.aws_s3_bucket.ingestion_s3.bucket
      USERNAME = "${var.db_username}"
      PASSWORD = "${var.db_password}"
      DATABASE = "${var.db_database}"
      HOST = "${var.db_host}"
      PORT = "${var.db_port}"
    }
  }

}
resource "aws_lambda_layer_version" "python_dotenv_layer" {
  layer_name = "python_dotenv_layer"
  filename = data.archive_file.layer.output_path
  
}

# Need to update with lambda main function filename
data "archive_file" "extract_lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/../src/extract_lambda.py"
  output_path = "${path.module}/../extract_lambda.zip"
}

data "archive_file" "layer" {
  type = "zip"
  source_dir = "${path.module}/../layer/"
  output_path = "${path.module}/../layer.zip"
}
