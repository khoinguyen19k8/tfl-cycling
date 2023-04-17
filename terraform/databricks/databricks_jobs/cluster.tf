# Cluster names
variable "all_purpose_cluster_name" {
  description = "A name for the cluster."
  type        = string
  default     = "all_purpose_cluster"
}

variable "work_cluster_name" {
  description = "A name for the cluster."
  type        = string
  default     = "work_cluster"
}

# Create the cluster with the "smallest" amount
# of resources allowed.
data "databricks_node_type" "all_purpose_databricks_node_type" {
  local_disk    = true
  min_memory_gb = 16
  min_cores     = 4
  category      = "General Purpose"
}

data "databricks_node_type" "work_databricks_node_type" {
  local_disk    = true
  min_memory_gb = 16
  min_cores     = 8
  category      = "Compute Optimized"
}

# Common configurations
variable "cluster_autotermination_minutes" {
  description = "How many minutes before automatically terminating due to inactivity."
  type        = number
  default     = 60
}

variable "cluster_num_workers" {
  description = "The number of workers."
  type        = number
  default     = 2
}

# Use the latest Databricks Runtime
# Long Term Support (LTS) version.
data "databricks_spark_version" "latest_lts" {
  long_term_support = true
}

resource "databricks_cluster" "all_purpose_databricks_cluster" {
  cluster_name            = var.all_purpose_cluster_name
  node_type_id            = data.databricks_node_type.all_purpose_databricks_node_type.id
  spark_version           = data.databricks_spark_version.latest_lts.id
  autotermination_minutes = var.cluster_autotermination_minutes
  num_workers             = var.cluster_num_workers
}

resource "databricks_cluster" "work_databricks_cluster" {
  cluster_name            = var.work_cluster_name
  node_type_id            = data.databricks_node_type.work_databricks_node_type.id
  spark_version           = data.databricks_spark_version.latest_lts.id
  autotermination_minutes = var.cluster_autotermination_minutes
  num_workers             = var.cluster_num_workers
}

output "all_purpose_cluster_url" {
  value = databricks_cluster.all_purpose_databricks_cluster.url
}

output "work_cluster_url" {
  value = databricks_cluster.work_databricks_cluster.url
}
