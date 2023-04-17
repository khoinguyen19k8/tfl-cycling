# Use environment variables for authentication.
provider "databricks" {}

# Retrieve information about the current user.
data "databricks_current_user" "me" {}
