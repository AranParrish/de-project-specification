# Defines variables used across the Terraform configurations

variable "s3_ingestion_name" {
    type = string
    default = "de-team-heritage-ingestion-zone"
}

variable "extract_lambda_name" {
    type = string
    default = "extract-lambda"
}

variable "s3_processed_data" {
    type = string
    default = "de-team-heritage-processed-data-zone"
}

variable "processed_lambda_name" {
    type = string
    default = "processed-lambda"
}

variable "load_lambda_name" {
    type = string
    default = "load-lambda"
}