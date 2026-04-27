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

        self.logger.info(f"Partition: {partition_date} | {start} → {end}")

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

        identifier = response.meta["identifier"]
        item_data = response.meta["item"]
        safe_id = identifier.replace("/", "_")

        # file_path = f"storage/pdf/{safe_id}.pdf"

        # with open(file_path, "wb") as f:
        #     f.write(response.body)

        # yield {
        #     "item_data": item_data,
        #     "file_path": file_path,
        #     "file_type": "pdf"
        # }
        yield {
            "item_data": item_data,
            "file_content": response.body,
            "file_type": "pdf"
        }
        
    def parse_html_page(self, response):

        identifier = response.meta["identifier"]
        item_data = response.meta["item"]
        safe_id = identifier.replace("/", "_")

        # content = response.css("div.container.mb-4").get() 
        content = response.css("body").get()

        if not content:
            content = response.text

        # file_path = f"storage/html/{safe_id}.html"

        # with open(file_path, "w", encoding="utf-8") as f:
        #     f.write(content)

        # yield {
        #     "item_data": item_data,
        #     "file_path": file_path,
        #     "file_type": "html"
        # }
        
        yield {
            "item_data": item_data,
            "file_content": content.encode("utf-8"), 
            "file_type": "html"
        }
        
        
    def open_spider(self, spider):
        log_event(
            "partition_started",
            partition=getattr(spider, "partition", None),
            body=getattr(spider, "body", None),
        )