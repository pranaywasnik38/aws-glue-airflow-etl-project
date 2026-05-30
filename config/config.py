# AWS_ACCESS_KEY = ""
# AWS_SECRET_KEY = ""
# AWS_REGION = "ap-south-1"
# RAW_BUCKET = "etl-project-raw-data-sample-bucket"

import os

from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION")

RAW_BUCKET = os.getenv("RAW_BUCKET")
PROCESSED_BUCKET = os.getenv("PROCESSED_BUCKET")

# SFTP
SFTP_HOST = "localhost"
SFTP_PORT = 2222
SFTP_USERNAME = "test"
SFTP_PASSWORD = "test"

# REST API
API_URL = "https://jsonplaceholder.typicode.com/users"

FINANCE_RAW_PATH = "sftp/finance/"
FINANCE_PROCESSED_PATH = "finance/"
API_RAW_PATH = "api/users"
API_PROCESSED_PATH = "users/"