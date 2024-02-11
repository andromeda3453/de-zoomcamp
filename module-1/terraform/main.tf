terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.s3_region
  alias  = "mc1"
}

provider "aws" {
  region = var.redshift_region
  alias  = "ue2"
}

resource "aws_s3_bucket" "s3-bucket" {
  bucket   = "s3-bucket-zoomcamp-demo"
  provider = aws.mc1
}

resource "aws_redshiftserverless_namespace" "test_namespace" {
  namespace_name = "test-namespace"
  provider       = aws.ue2
}