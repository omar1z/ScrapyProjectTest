import re

import scrapy
import os

from scrapyKedra.utils.logger import log_event

class KedraspiderSpider(scrapy.Spider):
    name = "kedraspider"
    allowed_domains = ["workplacerelations.ie"]

    def start_requests(self):

        bodies = {
            "Employment Appeals Tribunal": 2,
            "Equality Tribunal": 1,
            "Labour Court": 3,
            "Workplace Relations Commission": 15376
        }

        # Read dates from environment variables set by Dagster
        start = os.getenv("SCRAPY_START_DATE")
        end = os.getenv("SCRAPY_END_DATE")
        partition_date = os.getenv("SCRAPY_PARTITION_DATE")
        self.partition = partition_date

        # Fallback for running spider manually without Dagster
        if not start or not end:
            self.logger.warning("No dates from env, using defaults")
            start = "01/01/2024"
            end = "01/02/2024"
            partition_date = "2024-01-01"

        # Per-body counters — read by StatsPipeline at spider_closed
        self.body_stats = {
            body: {"found": 0, "scraped": 0, "failed": 0}
            for body in bodies
        }

        self.logger.info(f"Partition: {partition_date} | {start} → {end}")

        log_event(
            "partition_started",
            partition=partition_date,
            bodies=list(bodies.keys()),
        )

        base_url = os.getenv("base_url")

        for body_name, body_id in bodies.items():
            url = (
                f"{base_url}"
                f"?decisions=1&from={start}&to={end}&body={body_id}"
            )

            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta={
                    "body": body_name,
                    "partition": partition_date,
                    "start": start,
                    "end": end,
                }
            )

    def parse(self, response):

        items = response.css("li.each-item")
        body = response.meta.get("body")
        
        total_text = response.css("div.searchhead").get()
        match = re.search(r"of\s+([\d,]+)\s+results", total_text)
        total = int(match.group(1).replace(",", "")) if match else 0

        # Count every item found on this listing page toward this body
        if body and hasattr(self, "body_stats"):
            if self.body_stats[body]["found"] == 0:  # first page only
                self.body_stats[body]["total"] = total
            self.body_stats[body]["found"] += len(items)

        for item in items:
            item_data = {
                "title": item.css("p.description::attr(title)").get(),
                "descIdentifier": item.css("span.refNO::text").get(),
                "date": item.css("span.date::text").get(),
                "linkToDoc": item.css("a.btn.btn-primary::attr(href)").get(),
                "partition_date": response.meta.get("partition"),
                "body": response.meta.get("body"),
            }
            linkToDoc = item_data["linkToDoc"]
            linkToDoc = response.urljoin(linkToDoc)

            idd = item_data["descIdentifier"]

            if ".pdf" in linkToDoc or ".doc" in linkToDoc or ".docx" in linkToDoc:
                yield scrapy.Request(
                    url=linkToDoc,
                    callback=self.save_binary,
                    meta={
                        "identifier": idd,
                        "item": item_data
                    }
                )
            else:
                yield scrapy.Request(
                    url=linkToDoc,
                    callback=self.parse_html_page,
                    meta={"identifier": idd, "item":item_data}
                )

        next_page = response.css("a.next::attr(href)").get()

        if next_page:
            yield response.follow(
                next_page,
                callback=self.parse,
                meta=response.meta  
            )
            
    def save_binary(self, response):

        item_data = response.meta["item"]

        body = item_data.get("body")
        if body and hasattr(self, "body_stats"):
            self.body_stats[body]["scraped"] += 1

        yield {
            "item_data": item_data,
            "file_content": response.body,
            "file_type": "pdf"
        }
        
    def parse_html_page(self, response):

        item_data = response.meta["item"]

        body = item_data.get("body")
        if body and hasattr(self, "body_stats"):
            self.body_stats[body]["scraped"] += 1

        content = response.css("body").get()

        if not content:
            content = response.text

        yield {
            "item_data": item_data,
            "file_content": content.encode("utf-8"),
            "file_type": "html"
        }