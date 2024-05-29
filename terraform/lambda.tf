# Creates and configures the Lambda functions and their dependencies

# Creates and configures extract lambda function
resource "aws_lambda_function" "extract_lambda" {
    function_name = "${var.extract_lambda_name}"
    role = aws_iam_role.extract_lambda_role.arn
    filename=data.archive_file.extract_lambda_zip.output_path
    source_code_hash = data.archive_file.extract_lambda_zip.output_base64sha256
    layers = [aws_lambda_layer_version.python_dotenv_layer.arn]
    handler = "extract_lambda.lambda_handler"
    runtime = "python3.12"
    timeout = 300

# Add dependencies for lambda s3 access, cloudwatch access and secrets manager access
    depends_on = [
    aws_iam_role_policy_attachment.extract_lambda_s3_policy_attachment,
    aws_iam_role_policy_attachment.extract_lambda_cloudwatch_logs_policy,
    aws_iam_role_policy_attachment.extract_lambda_sm_policy_attachment
  ]

# Environment variables containing S3 ingestion zone bucket name
  environment {
    variables = {
      ingestion_zone_bucket = resource.aws_s3_bucket.ingestion_s3.bucket
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
  source_file = "${path.module}/../extract/src/extract_lambda.py"
  output_path = "${path.module}/../extract/extract_lambda.zip"
}

data "archive_file" "layer" {
  type = "zip"
  source_dir = "${path.module}/../extract/layer/"
  output_path = "${path.module}/../extract/layer.zip"
}


# For processed data zone

# Creates and configures the Lambda functions and their dependencies

resource "aws_lambda_function" "processed_lambda" {
    function_name = "${var.processed_lambda_name}"
    role = aws_iam_role.processed_lambda_role.arn 
    filename=data.archive_file.processed_lambda_zip.output_path 
    source_code_hash = data.archive_file.processed_lambda_zip.output_base64sha256
    layers = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:8"]
    handler = "processed_lambda.lambda_handler" 
    runtime = "python3.12"
    timeout = 900

# Add dependencies for lambda s3 access and cloudwatch access
    depends_on = [
    aws_iam_role_policy_attachment.processed_lambda_s3_policy_attachment,
    aws_iam_role_policy_attachment.processed_lambda_cloudwatch_logs_policy
  ]

# Environment variables containing S3 ingestion zone and processed zone bucket names
  environment {
    variables = {
      ingestion_zone_bucket = resource.aws_s3_bucket.ingestion_s3.bucket
      processed_data_zone_bucket = resource.aws_s3_bucket.processed_s3.bucket
    }
  }
}


data "archive_file" "processed_lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/../transform/src/processed_lambda.py"
  output_path = "${path.module}/../transform/processed_lambda.zip"
}


# For data warehouse

# Creates and configures the Lambda functions and their dependencies

resource "aws_lambda_function" "load_lambda" {
    function_name = "${var.load_lambda_name}"
    role = aws_iam_role.load_lambda_role.arn 
    filename=data.archive_file.load_lambda_zip.output_path 
    source_code_hash = data.archive_file.load_lambda_zip.output_base64sha256
    layers = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:8", aws_lambda_layer_version.python_dotenv_layer.arn]
    handler = "load_lambda.lambda_handler" 
    runtime = "python3.12"
    timeout = 900

# Add dependencies for load lambda s3 access, cloudwatch access, and secrets manager access
    depends_on = [
    aws_iam_role_policy_attachment.load_lambda_s3_policy_attachment,
    aws_iam_role_policy_attachment.load_lambda_cloudwatch_logs_policy,
    aws_iam_role_policy_attachment.load_lambda_sm_policy_attachment
  ]

# Environment variables containing S3 processed zone bucket name
  environment {
    variables = {
      processed_data_zone_bucket = resource.aws_s3_bucket.processed_s3.bucket
    }
  }
}


data "archive_file" "load_lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/../load/src/load_lambda.py"
  output_path = "${path.module}/../load/load_lambda.zip"
}
