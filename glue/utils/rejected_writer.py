from config.config import PROCESSED_BUCKET


def write_rejected_data(df, dataset_name):

    rejected_path = (
        f"s3a://{PROCESSED_BUCKET}/"
        f"rejected/{dataset_name}/"
    )

    (
        df.write
        .mode("append")
        .parquet(rejected_path)
    )