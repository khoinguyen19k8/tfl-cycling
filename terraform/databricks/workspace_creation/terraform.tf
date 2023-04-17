terraform {
  cloud {
    organization = "akooi"
    workspaces {
      name = "tfl-cycling"
    }
  }
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.16"
    }
  }

  required_version = ">= 1.2.0"
}
