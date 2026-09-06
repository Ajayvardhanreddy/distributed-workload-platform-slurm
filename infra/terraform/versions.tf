terraform {
  required_version = ">= 1.7.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  # Applied to every resource this provider creates. Makes it trivial to find
  # (and clean up) everything belonging to this lab in the AWS console/CLI.
  default_tags {
    tags = {
      Project   = "slurm-eda-lab"
      ManagedBy = "terraform"
    }
  }
}
