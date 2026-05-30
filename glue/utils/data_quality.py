from pyspark.sql.functions import col


def validate_not_empty(df):

    if df.count() == 0:
        raise Exception("Input dataframe is empty")


def split_valid_invalid(df, key_column):

    valid_df = df.filter(
        col(key_column).isNotNull()
    )

    invalid_df = df.filter(
        col(key_column).isNull()
    )

    return valid_df, invalid_df