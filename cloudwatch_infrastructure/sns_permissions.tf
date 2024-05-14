# Create IAM policy for SNS permissions
resource "aws_iam_policy" "sns_policy" {
  name        = "SNSPermissions"
  path        = "/"
  description = "IAM policy for SNS permissions"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "sns:Publish",
          "sns:Subscribe",
          "sns:CreateTopic",
          "sns:GetTopicAttributes",
          "sns:SetTopicAttributes",
          "sns:DeleteTopic",
          "sns:ListSubscriptionsByTopic"
        ]
        Resource = "*"
      }
    ]
  })
}

# Attach SNS policy to application role
resource "aws_iam_role_policy_attachment" "application_role_sns_policy" {
  policy_arn = aws_iam_policy.sns_policy.arn
  role       = aws_iam_role.application_role.name
}