provider "aws" {
    region="eu-west-2"

    default_tags {
      tags = {
        Project = "Totesys Olap"
        Team = "Heritage"
        Phase = "Extract"
      }
    }
}

terraform {
    backend "s3" {
      region = "eu-west-2"
      bucket= "de-team-heritage-terraform-statefiles"
      key = "extract-statefile"
    }
}
