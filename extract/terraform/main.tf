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

module "eventbridge" {
  source = "terraform-aws-modules/eventbridge/aws"

  bus_name = "my-bus"

  rules = {
    logs = {
      description   = "Capture log data"
      event_pattern = jsonencode({ "source" : ["my.app.logs"] })
    }
  }

  targets = {
    logs = [
      {
        name = "send-logs-to-sqs"
        arn  = aws_sqs_queue.queue.arn
      },
      {
        name = "send-logs-to-cloudwatch"
        arn  = aws_cloudwatch_log_stream.logs.arn
      }
    ]
  }
}