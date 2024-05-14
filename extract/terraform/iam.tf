resource "aws_iam_role" "extract_lambda_role" {
    
    name_prefix = "role-${var.lambda_name}"     # Need extract lambda name
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

# Attach totesys policy to extract_lambda role
resource "aws_iam_role_policy_attachment" "extract_lambda_totesys_policy_attachment" {
  role = aws_iam_role.extract_lambda_role.name
  policy_arn = aws_iam_policy.totesys_policy.arn
}
