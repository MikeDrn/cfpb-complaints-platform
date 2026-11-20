# Scheduled incremental ingestion — design

Code: `ingestion/incremental_collector.py`, `ingestion/pipeline.py`, `ingestion/service.py` · Schedule: NiFi process group **CFPB incremental schedule** (`ingestion/nifi/incremental_flow.json`)

## 1. Flow

```
NiFi (CRON 02:00 UTC daily)  --POST /run/incremental-->  collector service
   1. read high-water mark (ops/state/option=11/watermark.json)
   2. window = [mark - 21 days, today], one day per request, option filter, search_after paging
   3. store every response page in raw/cfpb/api/ingest_date=…/run_id=…/response_<day>_<page>.json
   4. write run log ops/runs/run_id=…/run.json
   5. Spark incremental job (merge with curated, deduplicate, quality gate)
   6. MongoDB load (upsert + aggregates + reconciliation)
   7. commit new high-water mark  ← only if 5 and 6 succeeded
```

## 2. Requirements (brief 6.2) and how they are met

| Requirement | Implementation |
|---|---|
| Agreed schedule | NiFi GenerateFlowFile, CRON `0 0 2 * * ?` (every 24 h at 02:00 UTC). Manual demo trigger = *Run Once* on the same flow, same code |
| Bounded request windows | One calendar day per request; window limited to `[mark − 21 days, today]` |
| Pagination | `search_after` cursor; page cap (200) and "cursor did not move" check guarantee the loop ends |
| Timeouts and temporary failures | 30 s timeout; retries on timeouts, connection errors, HTTP 429/5xx with back-off 2, 4, 8 s |
| Maximum retries | 4 attempts per request; other 4xx stop immediately |
| Raw response stored before processing | Every page, including empty pages, is written to MinIO before Spark runs |
| Empty response vs failed request | Empty day → `status: "empty"`, listed in `empty_days`, run `succeeded_empty`. Failed request → run `status: "failed"`, `error`, `failed_on_day`; high-water mark unchanged |
| Run log | `run.json`: run ID, start/end times, endpoint, non-secret parameters, window, pagination state per page (`search_after`), HTTP status, retry count, raw object paths, record counts, status |
| High-water mark | Latest `date_received` collected; committed only after processing and loading succeed; never moves backwards |
| Overlap and deduplication | 21-day overlap catches complaints published late. Spark deduplicates by Complaint ID; MongoDB upserts by Complaint ID |
| Which version is kept | API before full load, then latest ingestion (see `processing_rules.md`) |
| Replay without inflation | Replaying the same window leaves curated count and dashboard measures unchanged (demo step 5) |

## 3. Date field and older revisions

The collector selects records by **`date_received`** (the date the CFPB received the complaint). The overlap recovers complaints published up to 21 days late. It does **not** recover changes to older records, for example a company response updated months later. Those are reconciled by loading a new full CSV ZIP periodically (for example monthly): the historical flow stores it under a new `ingest_date`, and the Spark full run rebuilds the curated layer from it.
