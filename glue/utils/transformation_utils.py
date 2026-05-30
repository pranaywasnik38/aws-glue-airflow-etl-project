from pyspark.sql.functions import *
from pyspark.sql.types import *


def standardize_columns(df):

    for col_name in df.columns:

        new_name = (
            col_name.strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace(".", "_")
        )

        df = df.withColumnRenamed(
            col_name,
            new_name
        )

    return df


def add_audit_columns(df, source_system):

    return (
        df
        .withColumn(
            "source_system",
            lit(source_system)
        )
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