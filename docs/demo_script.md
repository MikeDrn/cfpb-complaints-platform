# Live demonstration — 15 minutes (brief section 11)

Prepare the day before: platform running (`docker compose up -d`), full load and Mongo already done, MinIO / NiFi / Compass / dashboard open in browser tabs, a terminal in the project folder.

| # | Brief step | Time | What to show | How |
|---|---|---|---|---|
| 1 | Architecture and scope | 2 min | Diagram; option 11 = payday loans under 3 historical labels; why each tool | Slide |
| 2 | Historical ingestion + manifest | 1.5 min | `raw/cfpb/full_load/ingest_date=2026-11-20/` with ZIP, CSV, `manifest.json` (19,316,897 records, SHA-256); NiFi historical flow and its provenance | MinIO console, NiFi |
| 3 | Bounded incremental run | 2 min | NiFi **CFPB incremental schedule** → right-click trigger → *Run Once*; then the new `raw/cfpb/api/…/run_id=…/response_*.json` and `ops/runs/run_id=…/run.json` | NiFi, MinIO |
| 4 | Spark, quality, reconciliation | 2 min | `quality/run_id=…/quality_report.json`: reconciliation equations, checks, flags; `docs/processing_rules.md` | MinIO / VS Code |
| 5 | Replay → no inflation | 2 min | Note the count; replay the same window; count unchanged | Commands below |
| 6 | MongoDB | 1.5 min | Compass: collections, indexes, one aggregation; `docs/index_benchmark.md` | Compass |
| 7 | Two pre-declared questions | 3 min | Q2 (issue mix by label period) and Q5 (Servicemember / Older American share) | Dashboard |
| 8 | Limitations + next improvement | 1 min | e.g. complaint counts ≠ borrowers; 2017 category change; next: population denominator or monthly full reload | Slide |

## Step 5 commands (replay)

```powershell
# count before
docker compose exec mongo mongosh -u admin -p YOURPASSWORD --quiet --eval "db.getSiblingDB('cfpb').complaints.countDocuments({})"
# replay the same bounded window twice (does not move the high-water mark)
docker compose run --rm jobs python -m ingestion.pipeline --start 2026-11-01 --end 2026-11-19
docker compose run --rm jobs python -m ingestion.pipeline --start 2026-11-01 --end 2026-11-19
# count after: identical; the quality report of the 2nd run shows duplicates removed, 0 conflicting values
docker compose exec mongo mongosh -u admin -p YOURPASSWORD --quiet --eval "db.getSiblingDB('cfpb').complaints.countDocuments({})"
```

Use a short window live (a few days) so it finishes in about a minute; show a longer saved run as evidence.
