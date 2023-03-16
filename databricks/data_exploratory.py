# Databricks notebook source
from pyspark.sql.functions import to_timestamp, date_format, coalesce
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType

# COMMAND ----------

access_key = dbutils.secrets.get(scope = "tfl-workspace", key = "AWS_ACCESS_KEY_ID")
secret_key = dbutils.secrets.get(scope = "tfl-workspace", key = "AWS_SECRET_ACCESS_KEY")
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)

# input_path = dbutils.widgets.get("input_path")
# output_path = dbutils.widgets.get("output_path")

input_path = "s3://tfl-cycling/raw/185JourneyDataExtract23Oct2019-29Oct2019.csv"
output_path = "s3://tfl-cycling/pq/"

# COMMAND ----------

df = spark \
    .read \
    .option("header", "True") \
    .option("inferSchema", "True") \
    .csv(input_path)

# COMMAND ----------

df.show()

# COMMAND ----------

df.count()

# COMMAND ----------

df.filter(df["EndStation Id"] == 215.0).show()

# COMMAND ----------

df.createOrReplaceTempView("df")

# COMMAND ----------

spark.sql("""
    SELECT `Rental Id`, AVG(Duration)
    FROM df
    GROUP BY `Rental Id`
""").show()
