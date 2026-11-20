# Spark processing rules — option 11 (payday lending)

Job: `processing/run_processing.py` · transformations: `processing/transform.py` · quality: `processing/quality.py`
Configuration: `config/scope_option11.json`, `config/issue_mapping.csv`, `config/quality_thresholds.json`

## 1. Steps (brief 6.3)

| # | Step | Rule |
|---|---|---|
| 1 | Read with a validated schema | CSV: explicit schema built from the manifest header, every column read as string; the run stops if any of the 15 required columns is missing. API: the run stops if `complaint_id`, `date_received` or `product` is missing. |
| 2 | Standardise | Column names → snake_case canonical schema shared by CSV and API (`company_response` → `company_response_to_consumer`, `timely` → `timely_response`). All text trimmed; `""`, `N/A`, `null`, `None` → null. State upper-cased. |
| 3 | Parse dates | `date_received`, `date_sent_to_company` → date (ISO first, then `MM/dd/yyyy`). |
| 4 | Option filter | Keep a record if it matches one of the three rules in `scope_option11.json`; add `label_version`. Original `product`, `sub_product` and `issue` are never changed. Other records are counted as **filtered out** (not rejected). |
| 5 | Label harmonisation | `issue_group` from `issue_mapping.csv`. Unknown issues → `Other / unmapped` + flag `unmapped_issue` (consistency warning). |
| 6 | Derived fields | `year`, `month`, `quarter`, `year_month`, `year_quarter`, `processing_lag_days`, `lag_status`, `tags_array`, `is_older_american`, `is_servicemember`. |
| 7 | Receipt-to-referral interval | `lag_status` = `ok`, `missing_date`, `invalid_date` or `negative`. Only `ok` records are used for interval measures. Negative intervals are flagged, not corrected. |
| 8 | Reject / flag | **Rejected** (quarantined in `quality/run_id=<id>/rejected/` with a reason): missing or non-numeric Complaint ID, missing or invalid date received. **Flagged** (kept, listed in `quality_flags`): missing/invalid/negative referral date, invalid timely value, missing/invalid state, future date, unmapped issue. |
| 9 | Deduplicate | One row per Complaint ID (see section 2). |
| 10 | Quality gate | Report written first; if any `fail` check, curated data is **not** published. |
| 11 | Publish | Curated Parquet `curated/complaints/option=11/year=YYYY/month=MM/` and aggregates `curated/aggregates/option=11/<name>/`. |

## 2. Deduplication rule

When the same Complaint ID appears more than once, the retained version is chosen in this order:

1. **API before full load** — the API is the more recent view of the complaint (e.g. a response that changed from "In progress").
2. **Latest ingestion timestamp** among versions from the same source.
3. **Latest date sent to company** as a final deterministic tie-breaker.

The quality report counts duplicate IDs, duplicate rows and IDs whose versions have **conflicting values** (same ID, different business fields). An identical replay creates duplicates with no conflict and changes nothing.

## 3. Reconciliation (checked on every run)

```
raw rows          = filtered out + in scope
in scope          = valid (new) + rejected
valid (all)       = curated + duplicates removed        (valid (all) = previously curated + valid new)
curated           = documents loaded into MongoDB        (checked by the loader)
raw rows (full)   = manifest record_count
```

## 4. Quality thresholds (set 20 Nov 2026, before interpreting results)

| Check | Threshold | Severity | Reason |
|---|---|---|---|
| Rejected records | ≤ 0.5 % of in-scope | fail | More would mean a parsing or source problem |
| Curated Complaint IDs unique | 0 duplicates | fail | Required by the brief |
| Reconciliation equations | all true | fail | Records must never disappear silently |
| Raw rows = manifest count | equal | fail | Proves Spark read the whole archive |
| Completeness: complaint_id, date_received, product | 100 % | fail | Needed to identify, date and scope a record |
| Completeness: issue, company | ≥ 99.5 % | fail | Needed for every analytical view |
| Completeness: timely response, company response | ≥ 99 % | fail | Needed for response measures |
| Sub-product where applicable (2017+ labels) | 100 % | fail | Our filter depends on it |
| Completeness: state, channel, referral date | ≥ 98–99 % | warn | Geographic/channel/interval views label missing values |
| Completeness: ZIP code | ≥ 90 % | warn | Optional; partially suppressed by the CFPB |
| Negative interval, invalid state | ≤ 0.5 % | warn | Reported with examples, records kept |
| Invalid timely value | ≤ 0.1 % | warn | Excluded from the timely percentage |
| Unmapped issue, future date | 0 % | warn | Signals a source category change to review |
| Freshness (latest date received) | ≤ 10 days old | warn | Publication delay is expected; more suggests a stalled collector |

## 5. Fields that do not apply by design

- `sub_product` is empty for every v1 record (the 2013–2017 "Payday loan" product had no sub-products). Completeness is therefore measured **where applicable** (v2 and v3 only).
- `sub_issue` is empty for most payday complaints because most payday issues have no sub-issue (4.8 % filled in the raw profile). This is reported, not treated as missing data.
- `company_public_response` is optional for companies (23.6 % filled).
