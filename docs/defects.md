# Defects found and corrected

Brief section 10 asks for "a brief account of defects found and corrected". Each entry: what happened → cause → fix → evidence.

## Intentional bad test records (detected by design)
| Record | Check that caught it | Evidence |
|---|---|---|
| Invalid date `2024-13-45` | Rejected, reason `invalid_date_received` | `tests/test_transform.py`, `tests/test_end_to_end.py`, `processing/make_sample.py` |
| Missing Complaint ID | Rejected, reason `missing_complaint_id` | same |
| Same Complaint ID with a different company response | Counted in `ids_with_conflicting_values`, one version kept by the dedup rule | `test_dedup_prefers_api_then_latest_and_counts_conflicts`, e2e test |
| Duplicate ID in curated output | Quality gate **fails** the run | `tests/test_quality.py::test_duplicate_ids_in_curated_output_fail_the_run` |

## Defects and environment problems
| # | Date | Problem | Cause | Fix | Evidence |
|---|---|---|---|---|---|
| 1 | 16 Nov | `docker pull minio/minio` → "pull access denied" | MinIO deleted its Docker Hub images (Sept 2026) | Chainguard MinIO image; run as root for volume permissions; bucket init retries instead of a health check | `docker-compose.yml` |
| 2 | 17 Nov | Collector would download page 1 again and again | API ignores `frm` / `page` | Cursor pagination with `search_after` | `api_notes.md` test E/F, `test_pagination_follows_search_after_cursor…` |
| 3 | 20 Nov | `sub_product=` filter returned all 19.3 M records | Parameter ignored by the API | `product=<parent>•<sub-product>` filter | `api_notes.md` 5.2 |
| 4 | 20 Nov | Dashboard / collector containers failed: "port is already allocated" | Containers of another project on 8501 and 8000 | Stopped them; collector host port moved to 8001 (NiFi uses the internal address) | `docker-compose.yml` |
| 5 | 20 Nov | NiFi download: `UnknownHostException` | Intermittent DNS inside Docker Desktop | Public DNS servers pinned in Compose | `docker-compose.yml` `dns:` |
| 6 | 20 Nov | NiFi download: Akamai "Access Denied" (curl from the same container: HTTP 200) | CDN refuses Java's default User-Agent | Request User-Agent and Accept header in NiFi; same headers in Python collectors (`HTTP_HEADERS`) | NiFi flow export, `config/settings.py` |
| 7 | 20 Nov | Finalise step: `collector: Name or service not known` | Collector restarting in a loop because host port 8000 was taken | See #4 | run log, manifest `landed_by: nifi` |
| 8 | 20 Nov | Index test reported an unknown plan (`?`) | MongoDB answered a pure count with `COUNT_SCAN` | Test groups by a document field; plan detection includes COUNT_SCAN / DISTINCT_SCAN | `tests/test_mongo.py` |
| 9 | 20 Nov | Query text showed dates as quoted strings | Display formatting only | `ISODate(...)` printed without quotes | `docs/query_results.md` |
| 10 | 20 Nov | Image rebuild timed out downloading dependencies | Adding one library invalidated the cached install of all of them | Test-only libraries in `requirements-dev.txt`, separate Docker layer | `docker/platform.Dockerfile` |
| 11 | 21 Nov | Dashboard headline 18,849 ≠ curated 19,043 | Default date filter started in 2014 | Default period = full data range; partial quarters labelled | `dashboard/app.py` |
| 12 | 21 Nov | Quality status `warn`: negative interval 520 %, invalid state 80 % | Incremental runs divided all-record flag counts by the number of new records | Denominator = all checked records; regression test | `processing/quality.py`, `test_flag_percentage_uses_all_checked_records…` |
| 13 | 21 Nov | Bright grid lines on the dark theme; unreadable quarter labels | Fixed light template; one label per quarter | Theme-aware charts; one label per year | `dashboard/app.py` |
