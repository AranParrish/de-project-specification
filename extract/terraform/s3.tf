resource "aws_s3_bucket" "ingestion_s3" {
  bucket_prefix = "${vars.s3_ingestion_name}-"

  tags = {
    Name        = "S3IngestionZone"
    Environment = "Dev"
  }
}


data "aws_iam_policy_document" "s3_policy_document" {
  statement {
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:ListObject",
      "s3:ListBucket",
    ]

    resources = [
      aws_s3_bucket.ingestion_s3.arn,
      "${aws_s3_bucket.ingestion_s3.arn}/*",
    ]
  }
}

resource "aws_iam_policy" "s3_policy" {
  name       = "s3_ingest_policy"
  policy = aws_iam_policy_document.s3_policy_document.json
}