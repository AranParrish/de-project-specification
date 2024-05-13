resource "aws_iam_policy" "totesys_policy" {
  name_prefix = "totesys_policy-"
  policy = jsondecode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect =  "Allow"
        Action =  [
            "rds:Describe*",
            "rds:ListTagsForResource",
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
            "cloudwatch:GetMetricStatistics",
            "cloudwatch:ListMetrics",
            "cloudwatch:GetMetricData",
            "logs:DescribeLogStreams",
            "logs:GetLogEvents",
        ]
        Resource = "*"
      }
    ]
  })
}