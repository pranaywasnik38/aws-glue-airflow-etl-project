from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import boto3

from glue.utils.spark_utils import configure_pyspark_for_windows
from config.config import RAW_BUCKET, PROCESSED_BUCKET

# ==========================================
# SPARK SESSION
# ==========================================

configure_pyspark_for_windows()

spark = (
    SparkSession.builder
    .appName("RegisterTransform")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.4.2,"
        "software.amazon.awssdk:bundle:2.29.52"
    )
    .getOrCreate()
)

# ==========================================
# AWS CREDENTIALS FOR S3A
# ==========================================

print("Loading AWS credentials...")

session = boto3.Session()
credentials = session.get_credentials()

if credentials is None:
    raise Exception("AWS credentials not found")

credentials = credentials.get_frozen_credentials()

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

hadoop_conf.set(
    "fs.s3a.access.key",
    credentials.access_key
)

hadoop_conf.set(
    "fs.s3a.secret.key",
    credentials.secret_key
)

if credentials.token:
    hadoop_conf.set(
        "fs.s3a.session.token",
        credentials.token
    )

hadoop_conf.set(
    "fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)

hadoop_conf.set(
    "fs.s3a.impl",
    "org.apache.hadoop.fs.s3a.S3AFileSystem"
)

hadoop_conf.set(
    "fs.s3a.endpoint",
    "s3.ap-south-1.amazonaws.com"
)

print("AWS credentials configured successfully")

# ==========================================
# READ RAW DATA
# ==========================================

raw_path = f"s3a://{RAW_BUCKET}/jdbc/registers/"

print(f"Reading customer data from: {raw_path}")

df = (
    spark.read
    .option("header", "true")
    .csv(raw_path)
)

print(f"Raw Count: {df.count()}")

df.show(5, truncate=False)

# ==========================================
# DATA CLEANING
# ==========================================

df_clean = (
    df.dropDuplicates()
      .na.fill("UNKNOWN")
)

# ==========================================
# AUDIT COLUMNS
# ==========================================

df_clean = (
    df_clean
    .withColumn(
        "processed_timestamp",
        current_timestamp()
    )
    .withColumn(
        "year",
        year(current_date())
    )
    .withColumn(
        "month",
        month(current_date())
    )
    .withColumn(
        "day",
        dayofmonth(current_date())
    )
)

# ==========================================
# WRITE PROCESSED DATA
# ==========================================

processed_path = f"s3a://{PROCESSED_BUCKET}/registers/"

print(f"Writing transformed data to: {processed_path}")

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

print("Customer transformation completed successfully")

spark.stop()