# Creates and configures S3 buckets

# ingestion zone bucket

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
  name       = "s3_policy"
  policy    = data.aws_iam_policy_document.s3_policy_document.json
}


# create s3 bucket for processed data zone
resource "aws_s3_bucket" "processed_s3" {
  bucket_prefix = "${var.s3_processed_data}-"

  tags = {
    Name        = "S3ProcessedZone"
    Environment = "Dev"
  }
}

# same iam_policy_document and policy for processed data zone and load lambda