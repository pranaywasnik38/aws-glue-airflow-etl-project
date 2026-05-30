from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from glue.utils.spark_utils import configure_pyspark_for_windows

from config.config import (
    AWS_ACCESS_KEY,
    AWS_SECRET_KEY,
    AWS_REGION,
    RAW_BUCKET,
    PROCESSED_BUCKET
)

# ==========================================
# SPARK CONFIG
# ==========================================

configure_pyspark_for_windows()

spark = (
    SparkSession.builder
    .appName("FinanceTransformation")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.262"
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
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    .getOrCreate()
)

# ==========================================
# RAW DATA LOCATION
# ==========================================

raw_path = f"s3a://{RAW_BUCKET}/sftp/finance/"

print(f"Reading finance data from: {raw_path}")

# ==========================================
# READ RAW DATA
# ==========================================

df = (
    spark.read
    .option("header", True)
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
      .na.fill(0)
)

df_clean = (
    df_clean
    .withColumn(
        "processed_timestamp",
        current_timestamp()
    )
)

# ==========================================
# EXTRACT PARTITIONS FROM FILE PATH
# ==========================================

df_clean = (
    df_clean
    .withColumn(
        "source_file",
        input_file_name()
    )
)

df_clean = (
    df_clean
    .withColumn(
        "year",
        regexp_extract(
            col("source_file"),
            r"year=(\d{4})",
            1
        )
    )
    .withColumn(
        "month",
        regexp_extract(
            col("source_file"),
            r"month=(\d{2})",
            1
        )
    )
    .withColumn(
        "day",
        regexp_extract(
            col("source_file"),
            r"day=(\d{2})",
            1
        )
    )
)

# ==========================================
# PROCESSED PATH
# ==========================================

processed_path = (
    f"s3a://{PROCESSED_BUCKET}/finance/"
)

print(f"Writing transformed data to: {processed_path}")

# ==========================================
# WRITE TO PROCESSED
# ==========================================

(
    df_clean.write
    .mode("append")
    .partitionBy(
        "year",
        "month",
        "day"
    )
    .parquet(processed_path)
)

print("Finance transformation completed successfully")

spark.stop()