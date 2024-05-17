# Defines IAM roles and attaches necessary policies to ensure Lambda functions have the required permissions to interact with S3, CloudWatch, and SNS
# By defining a specific IAM role for the Lambda function, you ensure that the Lambda function only has the permissions it needs to operate, 
# following the principle of least privilege. This enhances the security of your AWS environment

# Define the role and assume role policy
resource "aws_iam_role" "extract_lambda_role" {
    
    name_prefix = "role-${var.lambda_name}"
    assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

# Attach S3 policy to lambda function role
resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
    role = aws_iam_role.extract_lambda_role.name
    policy_arn = aws_iam_policy.s3_policy.arn
}

# Attach Secrets Manager policy to lambda function role
resource "aws_iam_role_policy_attachment" "lambda_sm_policy_attachment" {
    role = aws_iam_role.extract_lambda_role.name
    policy_arn = aws_secretsmanager_secret_policy.sm_policy.secret_arn
}

# Attach CloudWatch Logs policy to the extract_lambda role
resource "aws_iam_role_policy_attachment" "extract_lambda_cloudwatch_logs_policy" {
  role       = aws_iam_role.extract_lambda_role.name
  policy_arn = aws_iam_policy.cloudwatch_logs_policy.arn
}

# Attach SNS policy to extract_lambda role
resource "aws_iam_role_policy_attachment" "extract_lambda_role_sns_policy" {
  role       = aws_iam_role.extract_lambda_role.name
  policy_arn = aws_iam_policy.sns_policy.arn
}


