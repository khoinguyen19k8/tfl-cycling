# Databricks notebook source
from pyspark.sql.functions import to_timestamp, date_format, coalesce, to_date
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

access_key = dbutils.secrets.get(scope = "tfl-workspace", key = "AWS_ACCESS_KEY_ID")
secret_key = dbutils.secrets.get(scope = "tfl-workspace", key = "AWS_SECRET_ACCESS_KEY")
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)

# input_path = dbutils.widgets.get("input_path")
# output_path = dbutils.widgets.get("output_path")

input_path = "s3://tfl-cycling/raw/"
output_path = "s3://tfl-cycling/pq/"

# COMMAND ----------

df = spark \
    .read \
    .option("header", "True") \
    .option("inferSchema", "True") \
    .csv(input_path)

# COMMAND ----------

def to_date_(col, formats=("d/M/yyyy H:mm", "d/M/yyyy H:mm:ss")):
    # Spark 2.2 or later syntax, for < 2.2 use unix_timestamp and cast
    return coalesce(*[to_timestamp(col, f) for f in formats])

# COMMAND ----------

df_alt = df \
    .withColumn("Rental Id", df["Rental Id"].cast(IntegerType())) \
    .withColumn("Duration", df["Duration"].cast(DoubleType())) \
    .withColumn("Bike Id", df["Bike Id"].cast(IntegerType())) \
    .withColumn(
    "End Date",
    to_date_("End Date")
    ) \
    .withColumn("EndStation Id", df["EndStation Id"].cast(IntegerType())) \
    .withColumn("EndStation Name", df["EndStation Name"].cast(StringType())) \
    .withColumn(
    "Start Date",
    to_date_("Start Date")
    ) \
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

assert(df_alt.schema == schema)

# COMMAND ----------

df_alt.show()

# COMMAND ----------

df_alt.printSchema()

# COMMAND ----------

df_alt.createOrReplaceTempView("usage_stats")

# COMMAND ----------

spark.sql("""
    SELECT *
    FROM usage_stats
    WHERE DATE_PART('year', `End Date`) = 2015
    LIMIT 10
""").show()
