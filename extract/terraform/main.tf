provider "aws" {
    region="eu-west-2"

    default_tags {
      tags = {
        Project = totesys-olap
        Team = heritage
        Phase = extract
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
