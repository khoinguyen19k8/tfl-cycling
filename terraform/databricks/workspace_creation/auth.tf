provider "aws" {}

# Use environment variables for authentication.
provider "databricks" {
  alias = "mws"
  host  = "https://accounts.cloud.databricks.com"
}
