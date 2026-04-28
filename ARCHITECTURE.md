# Architecture Write-up

## Pipeline Overview

This project is structured around three main phases: **scraping**, **orchestration**, and **infrastructure**.

## Date Partition Size

Monthly partitions were chosen as the default unit because the WRC search API accepts `from`/`to` date filters, and a one-month window reliably keeps individual result sets small enough (typically a few hundred records) to stay within a single Scrapy run without hitting memory limits or triggering rate-limit responses from the server. Monthly boundaries also map cleanly to Dagster's `MonthlyPartitionsDefinition`, giving the UI a human-readable partition key (`2024-01-01`) that is easy to re-run or backfill. A smaller window (weekly) would multiply the number of Dagster runs by 4× with no practical benefit given the site's publication cadence, while a larger window (quarterly) risks oversized payloads and longer failure blast-radius — if one run fails, a month's worth of data is lost rather than three months'.

## Retries and Rate Limiting

Scrapy's built-in `RetryMiddleware` is active at its default (2 retries on 500/502/503/504 and connection errors). On top of that, `AutoThrottle` is enabled with a target concurrency of 4 and a maximum delay of 10 seconds, so the crawler dynamically backs off when the server responds slowly rather than hammering it with a fixed delay. `RandomUserAgentMiddleware` rotates the `User-Agent` header on every request to reduce fingerprinting. `CONCURRENT_REQUESTS_PER_DOMAIN` is capped at 8, and `DOWNLOAD_DELAY` is left at 0 so AutoThrottle alone governs pacing. Failed downloads are caught by `FailureLoggingMiddleware`, which emits a structured JSON log event (`request_failed`) with the URL and exception string so every failure is traceable without crashing the run.

## Deduplication Strategy

Idempotency is enforced at two levels. In the **landing zone**, `MongoPipeline` creates a unique compound index on `(descIdentifier, partition_date)` before the first write. On each item, it computes a SHA-256 hash of the raw file bytes. If a record already exists with the same hash, the item is skipped entirely and the file is not re-uploaded to MinIO. If the hash differs (content changed between runs), the MongoDB document is updated in-place and MinIO is overwritten. In the **processed zone**, `run_transformation` performs the same check before writing to the processed bucket and collection, additionally verifying that the MinIO object actually exists before declaring a skip. This means running the pipeline twice on the same date range produces zero duplicate records and zero redundant uploads.

## Scaling to 50+ Sources

The current design would need four changes to support 50+ sources cleanly:

1. **Source-driven configuration.** The bodies dict and base URL inside the spider would move to a YAML/JSON config file, with one entry per source. A factory function or a per-source spider class would replace the hardcoded `bodies` dict so each source can have its own pagination logic, CSS selectors, and authentication headers without forking the codebase.

2. **Per-source Dagster jobs/assets.** Rather than one monolithic `scraped_documents` asset, each source would become its own partitioned asset (or a dynamic job with `DynamicPartitionsDefinition`). This allows sources with different publication cadences (daily vs. monthly) to be scheduled independently and failed sources to be retried without touching others.

3. **Distributed execution.** Launching Scrapy as a subprocess works for a single source but becomes a bottleneck at scale. The pipeline would move to Scrapy Cluster or a queue-backed architecture (e.g. Redis + Scrapyd) so multiple spiders run concurrently on separate workers, with Dagster acting purely as the scheduler and dependency graph manager.

4. **Schema registry and validation.** With many sources producing heterogeneous metadata, a lightweight schema registry (e.g. a Pydantic model per source) would validate extracted fields before they reach MongoDB, catching selector drift early. A dead-letter collection for validation failures would replace silent `None` values in the current item dict.
