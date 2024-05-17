

data "aws_iam_policy_document" "sm_policy_doc" {
    statement {
        sid    = "LambdaToReadTheSecret"
        effect = "Allow"

        actions   = ["secretsmanager:GetSecretValue"]
        resources = ["arn:aws:lambda:*:*:function:SecretsManager*"]
    }

    # statement {
      
    #         sid = "BasePermissions"
    #         effect = "Allow"
    #         actions =  [
    #             "secretsmanager:*",
    #         ]
    #         resources = ["*"]
    #     }
    #     {
    #         sid = "LambdaPermissions"
    #         effect = "Allow"
    #         actions =  [
    #             "lambda:AddPermission"
    #             "lambda:CreateFunction"
    #             "lambda:GetFunction"
    #             "lambda:InvokeFunction"
    #             "lambda:UpdateFunctionConfiguration"
    #         ]
    #          resources = "arn:aws:lambda:*:*:function:SecretsManager*"
        
        
    # }
        
    
}


resource "aws_secretsmanager_secret_policy" "sm_policy" {
  secret_arn = "arn:aws:secretsmanager:eu-west-2:471112784619:secret:db_creds-t5KpJ3"
  policy     = data.aws_iam_policy_document.sm_policy_doc.json
}