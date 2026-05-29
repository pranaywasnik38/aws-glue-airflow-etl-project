import os
import logging
import requests
import pandas as pd

from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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

file_name = f"api_users_{timestamp}.csv"

local_directory = "data"

os.makedirs(local_directory, exist_ok=True)

local_file = f"{local_directory}/{file_name}"

# =========================================
# S3 KEY CONFIG
# =========================================

s3_key = (
    f"api/users/"
    f"year={datetime.now().year}/"
    f"month={datetime.now().month:02d}/"
    f"day={datetime.now().day:02d}/"
    f"{file_name}"
)

# =========================================
# RETRY SESSION
# =========================================

def create_retry_session():

    session = requests.Session()

    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[
            500,
            502,
            503,
            504
        ]
    )

    adapter = HTTPAdapter(
        max_retries=retry
    )

    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session

# =========================================
# API INGESTION
# =========================================

def ingest_api_data():

    try:

        logger.info("Creating retry session")

        session = create_retry_session()

        logger.info("Calling REST API")

        response = session.get(
            API_URL,
            timeout=30
        )

        logger.info(
            f"API Response Status: "
            f"{response.status_code}"
        )

        response.raise_for_status()

        logger.info("Converting API response to JSON")

        json_data = response.json()

        logger.info(
            "Converting JSON response to DataFrame"
        )

        df = pd.json_normalize(json_data)

        logger.info(
            f"Total records received: {len(df)}"
        )

        if df.empty:

            logger.warning(
                "No data received from API"
            )

            return

        logger.info("Previewing DataFrame")

        print(df.head())

        # =========================================
        # SAVE CSV LOCALLY
        # =========================================

        logger.info(
            f"Saving file locally: {local_file}"
        )

        df.to_csv(
            local_file,
            index=False
        )

        logger.info(
            "CSV file saved successfully"
        )

        # =========================================
        # UPLOAD TO S3
        # =========================================

        logger.info("Uploading file to S3")

        upload_file_to_s3(
            local_file,
            RAW_BUCKET,
            s3_key
        )

        logger.info(
            "API ingestion completed successfully"
        )

        logger.info(
            f"s3://{RAW_BUCKET}/{s3_key}"
        )

    except requests.exceptions.Timeout:

        logger.error(
            "API request timed out"
        )

        raise

    except requests.exceptions.RequestException as e:

        logger.error(
            f"API request failed: {str(e)}"
        )

        raise

    except Exception as e:

        logger.error(
            f"API ingestion failed: {str(e)}"
        )

        raise

# =========================================
# MAIN
# =========================================

if __name__ == "__main__":

    ingest_api_data()