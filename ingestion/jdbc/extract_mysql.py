import pandas as pd
import boto3
import logging

from sqlalchemy import create_engine
from datetime import datetime

from config.config import *

# =========================================
# LOGGING CONFIG
# =========================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s : %(message)s"
)

logger = logging.getLogger(__name__)

# =========================================
# MYSQL CONFIG
# =========================================

USERNAME = "root"
PASSWORD = "mysql1"
HOST = "localhost"
PORT = "3306"
DATABASE = "jdbc_db"

# =========================================
# GENERATE TIMESTAMP
# =========================================

timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

# =========================================
# FILE CONFIG
# =========================================

local_file = f"register_{timestamp}.csv"

s3_key = (
    f"jdbc/registers/"
    f"year={datetime.now().year}/"
    f"month={datetime.now().month:02d}/"
    f"day={datetime.now().day:02d}/"
    f"register_{timestamp}.csv"
)

# =========================================
# MYSQL CONNECTION
# =========================================

try:

    logger.info("Connecting to MySQL...")

    engine = create_engine(
        f"mysql+pymysql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
    )

    logger.info("MySQL connection successful")

    # =========================================
    # INCREMENTAL QUERY
    # =========================================

    query = """
    SELECT *
    FROM register
    WHERE updated_at >= DATE_SUB(NOW(), INTERVAL 1 DAY)
    """

    logger.info("Reading incremental data from MySQL...")

    df = pd.read_sql(query, engine)

    logger.info(f"Total records fetched: {len(df)}")

    if df.empty:
        logger.warning("No new records found")
        exit()

    logger.info("Previewing data")

    print(df.head())

    # =========================================
    # SAVE CSV LOCALLY
    # =========================================

    logger.info(f"Saving file locally: {local_file}")

    df.to_csv(local_file, index=False)

    logger.info("Local CSV file created successfully")

    # =========================================
    # CREATE S3 CLIENT
    # =========================================

    logger.info("Creating S3 client")

    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )

    # =========================================
    # UPLOAD TO S3
    # =========================================

    logger.info("Uploading file to S3...")

    s3.upload_file(
        local_file,
        RAW_BUCKET,
        s3_key
    )

    logger.info("File uploaded successfully")

    logger.info(f"s3://{RAW_BUCKET}/{s3_key}")

except Exception as e:

    logger.error(f"MySQL ingestion failed: {str(e)}")

    raise