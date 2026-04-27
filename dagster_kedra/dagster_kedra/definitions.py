from dagster import Definitions, define_asset_job, AssetSelection
from .assets.scrapyrun import scraped_documents
from .assets.transformation import transformed_documents

# Job 1 — ingestion only
ingestion_job = define_asset_job(
    name="ingestion_job",
    selection=AssetSelection.assets(scraped_documents),
)

# Job 2 — transformation only
transformation_job = define_asset_job(
    name="transformation_job",
    selection=AssetSelection.assets(transformed_documents),
)

# Job 3 — full pipeline (ingestion → transformation)
full_pipeline_job = define_asset_job(
    name="full_pipeline_job",
    selection=AssetSelection.all(),
)

defs = Definitions(
    assets=[scraped_documents, transformed_documents],
    jobs=[ingestion_job, transformation_job, full_pipeline_job],
)