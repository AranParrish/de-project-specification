# Defines variables used across the Terraform configurations

variable "s3_ingestion_name" {
    type = string
    default = "de-team-heritage-ingestion-zone"
}

variable "lambda_name" {
    type = string
    default = "extract-lambda"
}