# Creates and configures the Lambda functions and their dependencies

resource "aws_lambda_function" "extract_lambda" {
    function_name = "${var.lambda_name}" # Name of the Lambda function
    role = aws_iam_role.extract_lambda_role.arn # IAM role that provides the necessary permissions for the Lambda function to run
    filename=data.archive_file.extract_lambda_zip.output_path # Zip file containing the source code of the Lambda function
    source_code_hash = data.archive_file.extract_lambda_zip.output_base64sha256
    layers = [aws_lambda_layer_version.python_dotenv_layer.arn]
    handler = "extract_lambda.lambda_handler" # Entry point of the Lambda function
    runtime = "python3.12"
    timeout = 10

# Add dependencies for lambda s3 access, cloudwatch access, and eventbridge access
    depends_on = [
    aws_iam_role_policy_attachment.lambda_s3_policy_attachment,
    aws_iam_role_policy_attachment.extract_lambda_cloudwatch_logs_policy
  ]

# Environment variables containing database connection information and S3 bucket name
  environment {
    variables = {
      ingestion_zone_bucket = resource.aws_s3_bucket.ingestion_s3.bucket
      DATABASE = "${var.db_database}"
      HOST = "${var.db_host}"
      PORT = "${var.db_port}"
    }
  }
}

# Manages Lambda layers to include dependencies
resource "aws_lambda_layer_version" "python_dotenv_layer" {
  layer_name = "python_dotenv_layer"
  filename = data.archive_file.layer.output_path
  
}

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
