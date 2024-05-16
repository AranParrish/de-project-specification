variable "s3_ingestion_name" {
    type = string
    default = "de-team-heritage-ingestion-zone"
}

variable "lambda_name" {
    type = string
    default = "extract-lambda"
}

variable "db_username" {
    type = string
    sensitive = true
}

variable "db_password" {
    type = string
    sensitive = true
}

variable "db_database" {
    type = string
    default = "totesys"
}

variable "db_host" {
    type = string
    default = "nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com"
}

variable "db_port" {
    type = string
    default = "5432"
}
