import subprocess
import os
from pathlib import Path
from dagster import asset, AssetExecutionContext
from dagster_kedra.partitions import monthly_partitions

@asset(partitions_def=monthly_partitions)
def scraped_documents(context: AssetExecutionContext):

    # Dagster gives you the partition key e.g. "2024-01-01"
    partition_date = context.partition_key

    # Calculate end of month automatically
    from datetime import datetime
    from dateutil.relativedelta import relativedelta

    start_dt = datetime.strptime(partition_date, "%Y-%m-%d")
    end_dt = start_dt + relativedelta(months=1)

    # Format as DD/MM/YYYY 
    start_str = start_dt.strftime("%d/%m/%Y")   
    end_str = end_dt.strftime("%d/%m/%Y")       

    context.log.info(f"Scraping partition: {start_str} → {end_str}")

    base = Path(__file__).resolve().parents[3]
    scrapy_cfg = next(base.rglob("scrapy.cfg"), None)

    if scrapy_cfg is None:
        raise Exception("scrapy.cfg not found")

    # Pass dates as environment variables to the subprocess
    env = os.environ.copy()
    env["SCRAPY_START_DATE"] = start_str
    env["SCRAPY_END_DATE"] = end_str
    env["SCRAPY_PARTITION_DATE"] = partition_date  

    result = subprocess.run(
        ["scrapy", "crawl", "kedraspider"],
        cwd=str(scrapy_cfg.parent),
        capture_output=True,
        text=True,
        env=env
    )

    context.log.info(f"STDOUT:\n{result.stdout}")
    context.log.info(f"STDERR:\n{result.stderr}")

    if result.returncode != 0:
        raise Exception(f"Scrapy failed: {result.returncode}")

    context.log.info(f"Partition {partition_date} complete")