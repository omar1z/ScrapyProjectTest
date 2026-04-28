# Scraping Pipeline

A scalable scraping pipeline targeting legal texts from the [Workplace Relations Commission](https://www.workplacerelations.ie/en/search/) built with **Scrapy**, **Dagster**, and **Docker**.

---

## Prerequisites

Make sure you have the following installed before proceeding:

- [Docker](https://www.docker.com/products/docker-desktop)
- [Docker Compose](https://docs.docker.com/compose/)

No Python environment setup is needed — everything runs inside Docker.

---

## Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/omar1z/ScrapyProjectTest
cd ScrapyProjectTest
```

### 2. Configure environment variables

A `.env` file is included at the root of the project with all default values pre-configured. You can edit it to change partition dates, credentials, or bucket names before starting. (.env is kept in the repo since all data is virtualized, no security concerns)

```env
# Partition range — controls which months Dagster will make available
PARTITION_START=2024-01-01
PARTITION_END=2026-12-31

# MinIO credentials
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=password
MINIO_BUCKET=scrapy-data

# MongoDB
MONGO_DB=scrapy_db
MONGO_PROCESSED_DB=scrapy_db_processed
```

### 3. Start the stack

```bash
docker compose up --build
```

This will start four services:
- **Dagster** — orchestration UI and pipeline runner
- **MongoDB** — metadata storage (no setup needed, starts automatically)
- **MinIO** — object storage for scraped documents
- **MinIO Setup** — one-shot container that creates the bucket on first boot, then exits

> **Note:** On first run, Docker will build the image which may take a few minutes. Subsequent runs with `docker compose up` (without `--build`) will be faster.

---

## Accessing the Services

| Service | URL | Credentials |
|---|---|---|
| Dagster UI | http://localhost:3000 | — |
| MinIO Console | http://localhost:9001 | `admin` / `password` |

---

## Running the Pipeline

### Via Dagster UI (recommended)

1. Open **http://localhost:3000** in your browser
2. Navigate to **Assets** in the left sidebar
3. You will see two assets: `scraped_documents` and `transformed_documents`
4. Click **Materialize** to run the full pipeline, or use one of the three pre-defined jobs:

| Job | Description |
|---|---|
| `ingestion_job` | Runs scraping only |
| `transformation_job` | Runs transformation only (on already-scraped data) |
| `full_pipeline_job` | Runs scraping then transformation sequentially |

5. Select the partition(s) you want to process — each partition represents one calendar month (e.g. `2024-01-01` = January 2024)
6. Click **Launch Run**

---

## Inspecting the Data

### MongoDB

```bash
# Open a shell inside the MongoDB container
docker exec -it mongo mongosh

# Useful commands once inside
show dbs                                             # list all databases
use scrapy_db                                        # landing zone database
show collections
db.kedraspider.find().limit(5)                       # view 5 records
db.kedraspider.countDocuments()                      # count all records
db.kedraspider.find().sort({ _id: -1 }).limit(5)    # view most recent records

use scrapy_db_processed                              # processed zone database
db.kedraspider.countDocuments()
```

### MinIO

Open **http://localhost:9001** and log in with `admin` / `password`. You will find:

- **`scrapy-data`** bucket — landing zone with raw files organized as:
  - `html/` — raw HTML documents
  - `pdf/` — PDF and DOC files
  - `logs/` — structured JSON logs per body and per partition (logs only for the first job, scraping and ingestion, no logs for transformation for the sake of time)

- **`processed`** bucket — cleaned and renamed files organized the same way, produced by the transformation step

---

## Logs

Structured JSON logs are automatically stored in MinIO after each partition run under:

```
scrapy-data/logs/
├── 2024-01-01_Labour_Court.json
├── 2024-01-01_Equality_Tribunal.json
├── 2024-01-01_Employment_Appeals_Tribunal.json
├── 2024-01-01_Workplace_Relations_Commission.json
└── partition_2024-01-01_summary.json
```

Each body log contains: `records_found`, `records_scraped`, `records_failed`, and `timestamp`. The summary log adds overall Scrapy stats including total requests, HTTP 200 responses, 404s, and retries.

---

## Stopping the Stack

```bash
# Stop all containers (data is preserved in Docker volumes)
docker compose down

# Stop and delete all stored data (volumes)
docker compose down -v
```

> Data persists in named Docker volumes (`mongo_data`, `minio_data`) across restarts. It is only lost if you explicitly run `docker compose down -v`.
