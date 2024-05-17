# Creates and configures S3 buckets for ingested data

resource "aws_s3_bucket" "ingestion_s3" {
  bucket_prefix = "${var.s3_ingestion_name}-"

  tags = {
    Name        = "S3IngestionZone"
    Environment = "Dev"
  }
}


data "aws_iam_policy_document" "s3_policy_document" {
  statement {
    actions = [
      "s3:*",
      "s3-object-lambda:*",
    ]

    resources =  ["*"]
  }
}

resource "aws_iam_policy" "s3_policy" {
  name       = "s3_ingest_policy"
  policy    = data.aws_iam_policy_document.s3_policy_document.json
}