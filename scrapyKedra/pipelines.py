# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from datetime import datetime
import json
import os

from itemadapter import ItemAdapter
import pymongo
import hashlib
from scrapyKedra.config import MONGO_URI, MONGO_DB


class ScrapykedraPipeline:
    def process_item(self, item, spider):
        return item



from scrapyKedra.config import MONGO_URI, MONGO_DB


def calculate_hash(content: bytes) -> str:
    if not content:
        return None
    return hashlib.sha256(content).hexdigest()

class MongoPipeline:

    def __init__(self):
        self.mongo_uri = MONGO_URI
        self.mongo_db = MONGO_DB

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

        # Prevent duplicates on re-runs
        self.db[spider.name].create_index(
            [("descIdentifier", 1), ("partition_date", 1)],
            unique=True
        )

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        data = ItemAdapter(item).asdict()

        item_data = data.get("item_data", {})
        file_type = data.get("file_type")
        file_content = data.get("file_content")  

        # Calculate hash from file_content directly
        file_hash = calculate_hash(file_content)

        #remove body content, the content will be stored alone in bucket
        item_data.pop("file_content", None)

        if file_type:
            identifier = item_data.get("descIdentifier", "unknown").replace("/", "_")
            item_data["file_type"] = file_type
            item_data["file_key"] = f"{file_type}/{identifier}.{file_type}"

        # Add hash to metadata
        item_data["file_hash"] = file_hash        

        # Idempotency — check if record exists
        identifier = item_data.get("descIdentifier")
        partition_date = item_data.get("partition_date")

        existing = self.db[spider.name].find_one({
            "descIdentifier": identifier,
            "partition_date": partition_date
        })

        if existing:
            if existing.get("file_hash") == file_hash:
                # Same content -> skip
                spider.crawler.stats.inc_value("mongo/skipped_unchanged")
                spider.logger.info(f"Unchanged, skipping db insertion for: {identifier}")
                return item
            else:
                # Content changed -> update
                self.db[spider.name].update_one(
                    {"descIdentifier": identifier, "partition_date": partition_date},
                    {"$set": item_data}
                )
                spider.logger.info(f"Updated changed record: {identifier}")
        else:
            # New record → insert
            self.db[spider.name].insert_one(item_data)
            spider.logger.info(f"Inserted: {identifier}")

        return item
    
    
import io
from minio import Minio
from itemadapter import ItemAdapter
from scrapyKedra.config import (
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY
)


class S3Pipeline:

    def open_spider(self, spider):
        # MinIO client
        self.minio = Minio(
            MINIO_ENDPOINT,     
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        self.bucket = MINIO_BUCKET

        # MongoDB client for hash pre-check
        self.client = pymongo.MongoClient(MONGO_URI)
        self.db = self.client[MONGO_DB]

        try:
            if not self.minio.bucket_exists(self.bucket):
                self.minio.make_bucket(self.bucket)
                spider.logger.info(f"Bucket '{self.bucket}' created successfully")
        except Exception as e:
            spider.logger.error(f"Bucket check/create failed: {e}")

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        data = ItemAdapter(item).asdict()

        item_data = data.get("item_data", {})
        file_type = data.get("file_type")
        file_content = data.get("file_content")

        if not file_content:
            return item

        identifier = item_data.get("descIdentifier")
        partition_date = item_data.get("partition_date")
        incoming_hash = calculate_hash(file_content)

        # Check MongoDB for existing record with same hash
        existing = self.db[spider.name].find_one({
            "descIdentifier": identifier,
            "partition_date": partition_date
        })

        if existing and existing.get("file_hash") == incoming_hash:
            spider.crawler.stats.inc_value("minio/skipped_unchanged")
            spider.logger.info(f"Hash unchanged, skipping MinIO upload: {identifier}")
            return item

        # Hash differs or record is new then upload
        safe_identifier = identifier.replace("/", "_")
        file_key = f"{file_type}/{safe_identifier}.{file_type}"

        try:
            self.minio.put_object(
                self.bucket,
                file_key,
                io.BytesIO(file_content),
                length=len(file_content)
            )
            item["item_data"]["file_key"] = file_key
            item["item_data"]["file_type"] = file_type
            spider.logger.info(f"Uploaded to MinIO: {file_key}")

        except Exception as e:
            spider.logger.error(f"MinIO upload failed for {identifier}: {e}")
            item["item_data"]["file_key"] = None
            item["item_data"]["file_type"] = file_type

        return item
    
    
from scrapyKedra.utils.logger import log_event
from scrapy import signals


# class StatsPipeline:

#     @classmethod
#     def from_crawler(cls, crawler):
#         pipeline = cls()
#         crawler.signals.connect(pipeline.spider_closed, signal=signals.spider_closed)
#         pipeline.stats = crawler.stats
#         return pipeline

#     def spider_closed(self, spider):
#         stats = self.stats.get_stats()

#         log_event(
#             "partition_finished",
#             partition=getattr(spider, "partition", None),

#             items_scraped=stats.get("item_scraped_count", 0),
#             requests=stats.get("downloader/request_count", 0),
#             responses=stats.get("downloader/response_status_count/200", 0),
#             failed_requests=stats.get("downloader/response_status_count/404", 0),
#             retries=stats.get("retry/count", 0),
#         )


class StatsPipeline:
 
    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_closed, signal=signals.spider_closed)
        pipeline.stats = crawler.stats
        return pipeline
 
    def open_spider(self, spider):
        self.minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        self.bucket = MINIO_BUCKET
 
        try:
            if not self.minio_client.bucket_exists(self.bucket):
                self.minio_client.make_bucket(self.bucket)
        except Exception as e:
            spider.logger.error(f"Bucket check/create failed: {e}")
 
    def spider_closed(self, spider):
        stats = self.stats.get_stats()
        partition = getattr(spider, "partition", "unknown")
        body_stats = getattr(spider, "body_stats", {})
        timestamp = datetime.utcnow().isoformat()
 
        # ── One JSON log per body ──────────────────────────────────────
        for body, counts in body_stats.items():
            body_log = {
                "event": "body_finished",
                "partition": partition,
                "body": body,
                "records_found": counts["found"],
                "records_scraped": counts["scraped"],
                "records_failed": counts["failed"],
                "timestamp": timestamp,
            }
            encoded = json.dumps(body_log, indent=2).encode("utf-8")
            safe_body = body.replace(" ", "_")
            object_name = f"logs/{partition}_{safe_body}.json"
            try:
                self.minio_client.put_object(
                    bucket_name=self.bucket,
                    object_name=object_name,
                    data=io.BytesIO(encoded),
                    length=len(encoded),
                    content_type="application/json"
                )
                spider.logger.info(f"Body log uploaded: {object_name}")
            except Exception as e:
                spider.logger.error(f"MinIO body log upload failed ({body}): {e}")
 
        # ── Overall partition summary ──────────────────────────────────
        summary = {
            "event": "partition_finished",
            "partition": partition,
            "total_found": sum(c["found"] for c in body_stats.values()),
            "total_scraped": sum(c["scraped"] for c in body_stats.values()),
            "total_failed": sum(c["failed"] for c in body_stats.values()),
            "items_scraped": stats.get("item_scraped_count", 0),
            "requests": stats.get("downloader/request_count", 0),
            "responses_200": stats.get("downloader/response_status_count/200", 0),
            "failed_requests_404": stats.get("downloader/response_status_count/404", 0),
            "retries": stats.get("retry/count", 0),
            "timestamp": timestamp,
            "mongo_skipped": stats.get("mongo/skipped_unchanged", 0),  
            "minio_skipped": stats.get("minio/skipped_unchanged", 0),
        }
        encoded_summary = json.dumps(summary, indent=2).encode("utf-8")
        summary_key = f"logs/partition_{partition}_summary.json"
        try:
            self.minio_client.put_object(
                bucket_name=self.bucket,
                object_name=summary_key,
                data=io.BytesIO(encoded_summary),
                length=len(encoded_summary),
                content_type="application/json"
            )
            spider.logger.info(f"Summary log uploaded: {summary_key}")
        except Exception as e:
            spider.logger.error(f"MinIO summary log upload failed: {e}")