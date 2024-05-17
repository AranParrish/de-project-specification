# data "aws_secretsmanager_secret" "db-creds-arn" {
#   arn = "arn:aws:secretsmanager:eu-west-2:471112784619:secret:db_creds-t5KpJ3"
# }

# data "aws_iam_policy_document" "sm_policy_doc" {
#   statement {
#     sid    = "EnableAnotherAWSAccountToReadTheSecret"
#     effect = "Allow"

#     principals {
#       type        = "AWS"
#       identifiers = ["arn:aws:iam::471112784619:root"]
#     }

#     actions   = ["secretsmanager:GetSecretValue"]
#     resources = ["*"]
#   }
# }



