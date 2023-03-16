# Databricks notebook source
from pyspark.sql.functions import to_timestamp, date_format, coalesce
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType

# COMMAND ----------

access_key = dbutils.secrets.get(scope = "tfl-workspace", key = "AWS_ACCESS_KEY_ID")
secret_key = dbutils.secrets.get(scope = "tfl-workspace", key = "AWS_SECRET_ACCESS_KEY")
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)

input_path = dbutils.widgets.get("input_path")
output_path = dbutils.widgets.get("output_path")
num_partitions = int(dbutils.widgets.get("num_partitions"))

# COMMAND ----------

df = spark \
    .read \
    .option("header", "True") \
    .option("inferSchema", "True") \
    .csv(input_path)

# COMMAND ----------

df = df \
    .withColumn("Rental Id", df["Rental Id"].cast(IntegerType())) \
    .withColumn("Duration", df["Duration"].cast(DoubleType())) \
    .withColumn("Bike Id", df["Bike Id"].cast(IntegerType())) \
    .withColumn(
    "End Date", 
    date_format(
        coalesce(
            to_timestamp("End Date", "dd/MM/yyyy HH:mm"), 
            to_timestamp("End Date", "dd/MM/yyyy HH:mm:ss")
        ), 
        "yyyy-MM-dd HH:mm"
    )
    ) \
    .withColumn("EndStation Id", df["EndStation Id"].cast(IntegerType())) \
    .withColumn("EndStation Name", df["EndStation Name"].cast(StringType())) \
    .withColumn(
    "Start Date", 
    date_format(
        coalesce(
            to_timestamp("Start Date", "dd/MM/yyyy HH:mm"), 
            to_timestamp("Start Date", "dd/MM/yyyy HH:mm:ss")
        ), 
        "yyyy-MM-dd HH:mm"
    )
    ) \
    .withColumn(, to_timestamp("Start Date", "dd/MM/yyyy HH:mm")) \
    .withColumn("StartStation Id", df["StartStation Id"].cast(IntegerType())) \
    .withColumn("StartStation Name", df["StartStation Name"].cast(StringType()))

# COMMAND ----------

schema = StructType([
    StructField('Rental Id', IntegerType(), True), 
    StructField('Duration', DoubleType(), True), 
    StructField('Bike Id', IntegerType(), True), 
    StructField('End Date', TimestampType(), True), 
    StructField('EndStation Id', IntegerType(), True), 
    StructField('EndStation Name', StringType(), True), 
    StructField('Start Date', TimestampType(), True), 
    StructField('StartStation Id', IntegerType(), True), 
    StructField('StartStation Name', StringType(), True)
])

# COMMAND ----------

assert(df.schema == schema)

# COMMAND ----------

df \
    .repartition(num_partitions) \
    .write \
    .mode("overwrite") \
    .parquet(output_path)
