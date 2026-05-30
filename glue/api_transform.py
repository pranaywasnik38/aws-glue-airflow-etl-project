from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from glue.utils.spark_utils import configure_pyspark_for_windows

from config.config import (
    RAW_BUCKET,
    PROCESSED_BUCKET,
    API_RAW_PATH,
    API_PROCESSED_PATH,
    AWS_ACCESS_KEY,
    AWS_SECRET_KEY,
    AWS_REGION
)

# ==========================================
# WINDOWS SETUP
# ==========================================

configure_pyspark_for_windows()

# ==========================================
# SPARK SESSION
# ==========================================

spark = (
    SparkSession.builder
    .appName("ApiTransformation")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.4.1,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    )
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    .config(
        "spark.hadoop.fs.s3a.access.key",
        AWS_ACCESS_KEY
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        AWS_SECRET_KEY
    )
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        f"s3.{AWS_REGION}.amazonaws.com"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# ==========================================
# READ RAW DATA
# ==========================================

raw_path = f"s3a://{RAW_BUCKET}/{API_RAW_PATH}"

print(f"Reading API user data from: {raw_path}")

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(raw_path)
)

print(f"Raw Count: {df.count()}")

print("Schema:")
df.printSchema()

# ==========================================
# TRANSFORMATIONS
# ==========================================

df_clean = (
    df.dropDuplicates()
      .na.fill("UNKNOWN")
)

df_clean = (
    df_clean
    .withColumn("processed_timestamp", current_timestamp())
    .withColumn("year", year(current_date()))
    .withColumn("month", month(current_date()))
    .withColumn("day", dayofmonth(current_date()))
)

print(f"Transformed Count: {df_clean.count()}")

# ==========================================
# WRITE PROCESSED DATA
# ==========================================

processed_path = f"s3a://{PROCESSED_BUCKET}/{API_PROCESSED_PATH}"

print(f"Writing transformed data to: {processed_path}")

(
    df_clean.write
    .mode("append")
    .partitionBy("year", "month", "day")
    .parquet(processed_path)
)

print("API transformation completed successfully")

spark.stop()