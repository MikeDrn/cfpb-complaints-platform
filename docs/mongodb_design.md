# MongoDB serving layer — design

Database `cfpb` · loader `database/load_mongo.py` · indexes `database/indexes.py` · queries `database/queries.py`

## 1. Collections

| Collection | Content | Refreshed |
|---|---|---|
| `complaints` | One document per curated complaint (option 11, about 19,000) | Every load: upsert of all curated records by Complaint ID |
| `agg_quarterly` | Quarter × label period × issue group: complaints, timely Yes / valid, interval sum / valid count, tag counts | Rebuilt every load (temp collection + rename) |
| `agg_state_issue` | State × issue group: complaints | Rebuilt every load |
| `agg_response` | Year × issue group × company response: complaints, timely Yes / valid | Rebuilt every load |
| `agg_channel` | Year × submission channel: complaints | Rebuilt every load |
| `meta` | `freshness` (latest complaint date, latest ingestion, serving refresh time, reconciliation) and `quality_latest` (last quality report) | Every load |

Raw data never enters MongoDB. The aggregates come from Spark (`curated/aggregates/option=11/`), so the dashboard's headline numbers and the curated layer always agree.

## 2. Complaint document

```json
{
  "_id": 30990964,                       // Complaint ID, int64 — unique by definition of _id
  "complaint_id": 30990964,
  "date_received": ISODate("2026-10-26"), // native BSON date (UTC midnight)
  "date_sent_to_company": ISODate("2026-10-26"),
  "year": 2026, "quarter": 4, "month": 10, "year_quarter": "2026-Q4", "year_month": "2026-10",
  "product": { "name": "Payday loan, title loan, personal loan, or advance loan",
               "sub_product": "Payday loan", "label_version": "v3_2023_now" },
  "issue":   { "name": "Charged fees or interest you didn't expect", "sub_issue": null,
               "group": "Unexpected fees or interest" },
  "company": { "name": "…", "response_to_consumer": "Closed with explanation",
               "public_response": null, "timely_response": true },
  "location": { "state": "TX", "zip_code": "799XX" },
  "submitted_via": "Web",
  "tags": ["Servicemember"], "is_servicemember": true, "is_older_american": false,
  "processing_lag_days": 0, "lag_status": "ok",
  "quality_flags": [],
  "lineage": { "source": "full_load", "run_id": "full_2026-11-20", "ingest_date": "2026-11-20",
               "raw_object": "s3a://raw/cfpb/full_load/ingest_date=2026-11-20/complaints.csv" },
  "loaded_at": ISODate("2026-11-20T20:10:00Z")
}
```

### Design decisions
- **Embedding:** product, issue, company and location are small sub-documents that are always read with the complaint and change together, so they are embedded (Session 4: "data accessed together is stored together"). There is no unbounded array, so nothing needs a separate referenced collection.
- **Original and harmonised labels side by side:** `issue.name` is the CFPB text; `issue.group` is our mapping. `product.label_version` records which historical label the record came from.
- **Dates as BSON dates, not strings:** range filters (`$gte`), sorting and index ordering work correctly; strings would compare alphabetically and use more space.
- **Timely response as true / false / null:** null means missing or invalid, so the timely percentage divides by valid Yes + No only.
- **Complaint ID as int64 `_id`:** MongoDB enforces uniqueness on `_id` automatically. It is stored as int64 because Python would otherwise store IDs below 2³¹ as int32.
- **Schema validation:** a `$jsonSchema` validator rejects documents with wrong types (e.g. a date stored as text). Tested in `tests/test_mongo.py`.

## 3. Loading — why repeated runs are safe
- `ReplaceOne(filter={_id}, upsert=True)` in bulk batches of 1,000: loading the same records twice leaves one document per Complaint ID with the latest curated values.
- Aggregates are built in a temporary collection and renamed over the old one, so the dashboard never reads a half-built aggregate.
- After loading, the loader compares curated records with MongoDB documents and stops with an error if they differ. The result is stored in `meta.freshness.reconciled`.

## 4. Indexes (built for the dashboard's filters)

| Index | Supports |
|---|---|
| `_id` (automatic, unique) | Lookups and upserts by Complaint ID |
| `date_received` | Date filter alone |
| `issue.group + date_received` | Issue filter + date range |
| `location.state + date_received` | State filter + date range |
| `company.name + date_received` | Company filter + date range |
| `submitted_via + date_received` | Channel filter + date range |
| `product.label_version + date_received` | Label-period comparison |
| `tags + date_received` (multikey) | Servicemember / Older American filter |

Equality field first, then the date range (MongoDB ESR rule). Every index speeds up some reads but costs storage and adds work to each write; we index only fields the dashboard filters on. Measured effect: `docs/index_benchmark.md`.
