from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ReadProcessedData")
    .getOrCreate()
)

df = spark.read.parquet(
    "s3a://etl-project-processed-data-sample-bucket/registers/"
)

df.printSchema()
df.show(truncate=False)

spark.stop()