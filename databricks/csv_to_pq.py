# Databricks notebook source
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType

# COMMAND ----------

access_key = dbutils.secrets.get(scope = "tfl-workspace", key = "AWS_ACCESS_KEY_ID")
secret_key = dbutils.secrets.get(scope = "tfl-workspace", key = "AWS_SECRET_ACCESS_KEY")
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
output_path = "s3://tfl-cycling/pq/"
