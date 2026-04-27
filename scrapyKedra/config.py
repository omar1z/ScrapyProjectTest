import os
from dotenv import load_dotenv

load_dotenv()  

BASE_URL = os.getenv("base_url")

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")

MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")

HTML_PATH = os.getenv("STORAGE_HTML_PATH")
PDF_PATH = os.getenv("STORAGE_PDF_PATH")


PARTITION_START = os.getenv("PARTITION_START")
PARTITION_END = os.getenv("PARTITION_END")