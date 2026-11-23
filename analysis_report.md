# Train Delay Analytics Pipeline — Sample Analysis Report

> **Data notice:** This report uses simulated demonstration records, not live Rail Radar or Indian Railways data.

## Executive summary
This project demonstrates an incremental pipeline concept for collecting train-running status, retaining active trains that have reached or passed a route midpoint, and landing source snapshots in a raw layer. The included dataset contains **80 simulated records**, of which **66** are marked active and **66** meet the active + 50% progress filter.

## Sample run metrics
- Raw records: 80
- Active records: 66
- Filtered mid-route records: 66
- Filtered records delayed over 15 minutes: 19
- Mean delay (filtered set): 31.8 minutes
- Mean route progress (filtered set): 72.2%
- Mean current speed (filtered set): 68.9 km/h
- Delay classes: {"On time/Minor": 47, "Moderate": 5, "Major": 14}

## Pipeline stages
1. **Extract:** configurable API client (provider endpoint/schema must be verified before live use).
2. **Raw landing:** immutable JSON snapshots organized by date.
3. **Normalize:** map provider fields into a stable schema.
4. **Filter:** `is_live = true` and `progress_percent >= 50`.
5. **Chunk:** process filtered records in configured batch sizes.
6. **Deduplicate:** deterministic snapshot-key checks; this is snapshot-level duplicate protection.
7. **Storage:** local dry-run storage by default; S3 is configurable.
8. **Orchestration:** Airflow DAG schedule.

## Interpretation and limitations
The output is a demonstration, not a performance evaluation of actual rail operations. Synthetic delay, speed, train names, station labels and timestamps were generated to exercise the workflow. Route progress is represented as a percentage; a production source must provide or enable trustworthy progress/route-distance fields. The API endpoint, credentials, rate limits and field mappings must be verified.

## Suggested production enhancements
- Confirm API contract and add schema validation.
- Add retries/backoff, rate-limit handling, observability and dead-letter output.
- Build S3 partitioning by snapshot date and optional train/date key.
- Add a curated analytics layer (Parquet) and Athena tables.
- Add data quality checks and Airflow task-level metrics.
- Extend deduplication from snapshot keys to a train/date/event strategy if needed.
