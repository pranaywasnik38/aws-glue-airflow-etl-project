import os
import logging
import pysftp

from datetime import datetime

from ingestion.utils.s3_utils import upload_file_to_s3
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
# GENERATE TIMESTAMP
# =========================================

timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

# =========================================
# FILE CONFIG
# =========================================

file_name = f"finance_data_{timestamp}.csv"

local_directory = "data"

os.makedirs(local_directory, exist_ok=True)

local_file = f"{local_directory}/{file_name}"

remote_file = "/upload/finance_data_pandas_practice.csv"

# =========================================
# S3 KEY CONFIG
# =========================================

s3_key = (
    f"sftp/finance/"
    f"year={datetime.now().year}/"
    f"month={datetime.now().month:02d}/"
    f"day={datetime.now().day:02d}/"
    f"{file_name}"
)

# =========================================
# SFTP INGESTION
# =========================================

def ingest_sftp_data():

    try:

        logger.info("Connecting to SFTP server...")

        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

        with pysftp.Connection(
            host=SFTP_HOST,
            username=SFTP_USERNAME,
            password=SFTP_PASSWORD,
            port=SFTP_PORT,
            cnopts=cnopts
        ) as sftp:

            logger.info("SFTP connection successful")

            logger.info("Downloading file from SFTP...")

            sftp.get(
                remote_file,
                local_file
            )

            logger.info("File downloaded successfully")

        # =========================================
        # UPLOAD TO S3
        # =========================================

        logger.info("Uploading file to S3...")

        upload_file_to_s3(
            local_file,
            RAW_BUCKET,
            s3_key
        )

        logger.info("SFTP ingestion completed successfully")

        logger.info(f"s3://{RAW_BUCKET}/{s3_key}")

    except Exception as e:

        logger.error(f"SFTP ingestion failed: {str(e)}")

        raise


if __name__ == "__main__":
    ingest_sftp_data()