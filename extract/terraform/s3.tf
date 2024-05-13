resource "aws_s3_bucket" "ingestion_s3" {
  bucket_prefix = "${vars.s3_ingestion_name}-"

  tags = {
    Name        = "S3IngestionZone"
    Environment = "Dev"
  }
}

resource "aws_s3_bucket_policy" "s3_ingestion_policy" {
  bucket = aws_s3_bucket.example.id
  policy = data.aws_iam_policy_document.s3_policy_doc.json
}

data "aws_iam_policy_document" "s3_policy_doc" {
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