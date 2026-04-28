import os
import io
import hashlib
from pathlib import Path
from dagster import asset, AssetExecutionContext
from dagster_kedra.partitions import monthly_partitions
from pymongo import MongoClient
from minio import Minio
from bs4 import BeautifulSoup
import re


MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.getenv("MONGO_DB", "scrapy_db")
MONGO_PROCESSED_DB = os.getenv("MONGO_PROCESSED_DB", "scrapy_db_processed") 
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_LANDING_BUCKET = os.getenv("MINIO_BUCKET", "landing-zone")
MINIO_PROCESSED_BUCKET = os.getenv("MINIO_PROCESSED_BUCKET", "processed")


def calculate_hash(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def clean_html(file_content: bytes) -> bytes:
    soup = BeautifulSoup(file_content, "html.parser")

    for tag in soup(["nav", "header", "footer", "button", "script", "style"]):
        tag.decompose()

    # Remove cookie banner specifically
    for div in soup.find_all("div", {"class": lambda c: c and "cookie" in " ".join(c).lower()}):
        div.decompose()

    main = (
        soup.find(id="main")                                                     
        or soup.find("div", {"class": lambda c: c and "mb-4" in c})             
        or soup.find("div", {"class": lambda c: c and "container" in c})        
        or soup.find("main")                                                    
        or soup.body
    )

    return str(main).encode("utf-8") if main else file_content



def clean_text(text: str) -> str:
    if not text:
        return text
    # Remove \n and \r
    text = text.replace("\n", " ").replace("\r", " ")
    text = re.sub(r" {2,}", " ", text)
    return text.strip()


def run_transformation(partition_date: str, log=None):

    def info(msg):
        if log:
            log.info(msg)
        else:
            print(msg)

    client = MongoClient(MONGO_URI)

    db = client[MONGO_DB]                       
    landing = db["kedraspider"]

    processed_db = client[MONGO_PROCESSED_DB]   
    processed = processed_db["kedraspider"]

    minio = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    # Auto-create processed bucket
    if not minio.bucket_exists(MINIO_PROCESSED_BUCKET):
        minio.make_bucket(MINIO_PROCESSED_BUCKET)
        info(f"Created bucket: {MINIO_PROCESSED_BUCKET}")

    records = list(landing.find({"partition_date": partition_date}))
    info(f"Found {len(records)} records for partition {partition_date}")

    for record in records:
        identifier = record.get("descIdentifier", "unknown").replace("/", "_")
        file_type = record.get("file_type")
        file_key = record.get("file_key")

        if not file_key:
            info(f"No file_key for {identifier}, skipping")
            continue


        try:
            response = minio.get_object(MINIO_LANDING_BUCKET, file_key)
            file_content = response.read()
        except Exception as e:
            info(f"Failed to fetch {file_key}: {e}")
            continue


        if file_type == "html":
            final_content = clean_html(file_content)
        else:
            final_content = file_content    # pdf/doc no transformation

        new_key = f"{file_type}/{identifier}.{file_type}"
        new_hash = calculate_hash(final_content)

        # Idempotency check
        existing = processed.find_one({
            "descIdentifier": record.get("descIdentifier"),
            "partition_date": partition_date
        })
        if existing and existing.get("file_hash") == new_hash:
            try:
                minio.stat_object(MINIO_PROCESSED_BUCKET, new_key)
                info(f"Unchanged, skipping: {identifier}")
                continue
            except Exception:
                info(f"File missing from MinIO, reprocessing: {identifier}")

        
        minio.put_object(
            MINIO_PROCESSED_BUCKET,
            new_key,
            io.BytesIO(final_content),
            length=len(final_content)
        )

        # Upsert metadata into processed collection
        processed.update_one(
            {
                "descIdentifier": record.get("descIdentifier"),
                "partition_date": partition_date
            },
            {"$set": {
                **{k: v for k, v in record.items() if k != "_id"},
                "title": clean_text(record.get("title", "")),
                "file_hash": new_hash,
                "file_key": new_key,
                "processed_bucket": MINIO_PROCESSED_BUCKET,
            }},
            upsert=True
        )
        info(f"Processed: {new_key}")

    client.close()
    info(f"Transformation complete for partition: {partition_date}")


# ── Dagster Asset ───────────────────────────────────────────────────
@asset(
    partitions_def=monthly_partitions,
    deps=["scraped_documents"]
)
def transformed_documents(context: AssetExecutionContext):
    partition_date = context.partition_key
    context.log.info(f"Starting transformation for: {partition_date}")

    # Pass Dagster logger into the function
    run_transformation(partition_date, log=context.log)

    context.log.info(f"Transformation complete: {partition_date}")


# Standalone (run directly without Dagster)
# if __name__ == "__main__":
#     partition = os.getenv("TRANSFORM_PARTITION_DATE", "2024-01-01")
#     run_transformation(partition)