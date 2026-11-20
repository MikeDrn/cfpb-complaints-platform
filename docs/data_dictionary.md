# Data dictionary

**Source:** CFPB Consumer Complaint Database — field reference https://cfpb.github.io/api/ccdb/fields.html (reviewed 17 November 2026) and API observations in `api_notes.md`.
**Licence:** CC0 (public domain).
**Removed from the source (not used by this project):** consumer complaint narrative (removed September 2026), consumer disputed, consumer consent provided (removed from exports June 2026).

## 1. Source fields → canonical schema

| Canonical name | CSV header | API field | Type (curated) | Official meaning | How we use it | Caveats |
|---|---|---|---|---|---|---|
| `complaint_id` | Complaint ID | `complaint_id` (string) | long | Unique identification number for a complaint | Deduplication key; MongoDB `_id`; lineage | Rows with missing or non-numeric IDs are rejected |
| `date_received` | Date received | `date_received` (UTC timestamp) | date | Date the CFPB received the complaint | Trends, year/month partitions, watermark | Recent periods are incomplete because of publication delay |
| `date_sent_to_company` | Date sent to company | `date_sent_to_company` (UTC timestamp) | date | Date the CFPB sent the complaint to the company | Processing lag | Not the date the company resolved the complaint |
| `product` | Product | `product` | string | Type of product the consumer identified (categorical) | Option filter (kept as the original label) | Payday loans appear under three product labels over time (see `label_version`) |
| `sub_product` | Sub-product | `sub_product` | string | Type of sub-product (categorical) | Option filter: must be "Payday loan" for the 2017+ labels | Empty for the 2013–2017 "Payday loan" product, which had no sub-products |
| `issue` | Issue | `issue` | string | Issue the consumer identified; values depend on product | Kept as the original label; mapped to `issue_group` | Wording changed in April 2017 (e.g. "I didn't expect" → "you didn't expect") |
| `sub_issue` | Sub-issue | `sub_issue` | string | Sub-issue; values depend on product and issue | Drill-down | Not all issues have sub-issues |
| `company` | Company | `company` | string | The company the complaint is about | Company-level summaries | Volume depends on company size: never a quality ranking |
| `company_public_response` | Company public response | `company_public_response` | string | Optional public response chosen from a pre-set list, up to 180 days after sending | Context only | Mostly "chooses not to provide a public response" or null |
| `company_response_to_consumer` | Company response to consumer | `company_response` | string | How the company responded, e.g. "Closed with explanation" | Response-pattern analysis | Not customer satisfaction; "In progress" changes later |
| `timely_response` | Timely response? | `timely` | string Yes/No (bool in MongoDB) | Whether the company gave a timely response | % timely | Not resolution time or satisfaction |
| `state` | State | `state` | string | State of the consumer's mailing address | Geographic comparison | Includes territories and military codes; counts not adjusted for population |
| `zip_code` | ZIP code | `zip_code` | string | Mailing ZIP code: 5 digits, or 3 digits when the ZCTA has fewer than 20,000 people, or empty | Stored only | Partially suppressed; never displayed at record level (re-identification risk) |
| `submitted_via` | Submitted via | `submitted_via` | string | How the complaint was submitted | Channel mix | Values: Web, Referral, Phone, Postal mail, Fax, Email |
| `tags` | Tags | `tags` (string) | string | "Older American" (consumer aged 62+), "Servicemember" (active duty, Guard, Reserve, veteran, retiree, or spouse/dependent), or both | Segment summaries | Comma-separated in source; null for most records |

## 2. Derived fields (created by Spark)

| Name | Type | Derivation | Purpose |
|---|---|---|---|
| `year` | int | `year(date_received)` | Partitioning, yearly measures |
| `month` | int | `month(date_received)` | Partitioning, monthly measures |
| `quarter` | int | `quarter(date_received)` | Quarterly trends |
| `year_quarter` | string `YYYY-Qn` | year + quarter | Main trend level for option 11 (monthly counts are sparse) |
| `year_month` | string `YYYY-MM` | `date_format(date_received)` | Time series, incremental load scope |
| `processing_lag_days` | int | `date_sent_to_company − date_received` in days | CFPB routing lag (explicitly **not** company resolution time) |
| `label_version` | string | which scope rule matched: `v1_2013_2017`, `v2_2017_2023`, `v3_2023_now` (`config/scope_option11.json`) | Traces records to their historical product label |
| `issue_group` | string | `issue` looked up in `config/issue_mapping.csv`; unknown issues → `Other / unmapped` and counted in the quality report | Comparisons across the 2017 wording change: unexpected fees or interest, bank account withdrawals, repayment and payoff, contacting the lender, loan origination, credit reporting |
| `tags_array` | array<string> | split `tags` on commas | MongoDB multikey index, segment filters |
| `is_older_american` | boolean | `tags_array` contains "Older American" | Segment analysis |
| `is_servicemember` | boolean | `tags_array` contains "Servicemember" | Segment analysis |
| `quality_flags` | array<string> | rule checks: `invalid_date_sent`, `invalid_timely_value`, `invalid_state`, `negative_processing_lag`, `future_date_received` | Data-quality reporting without dropping records |
| `reject_reason` | string (rejected set only) | `missing_or_invalid_complaint_id`, `missing_date_received`, `invalid_date_received` | Quarantine explanation |

## 3. Lineage fields

| Name | Meaning |
|---|---|
| `_src` | `full_load` (CSV ZIP) or `api` (incremental) |
| `_src_priority` | 1 = full load, 2 = API; higher wins during deduplication |
| `_ingest_date` | Date the raw data was collected |
| `_run_id` | Ingestion run identifier (links to `ops/runs/run_id=…/run.json`) |
| `_ingest_ts` | Timestamp of collection; second tie-breaker in deduplication |

## 4. Categorical reference values (observed 17 November 2026)

- **Submitted via:** Web, Referral, Phone, Postal mail, Fax, Email.
- **Company response to consumer:** Closed with explanation, Closed with non-monetary relief, In progress, Closed with monetary relief, Untimely response, Closed without relief, Closed, Closed with relief.
- **Timely response?:** Yes, No.
- **Tags:** Older American, Servicemember, Older American + Servicemember.

## 5. Option 11 scope rule

A record is in scope when **one** of these is true (stored in `config/scope_option11.json`):

| Label version | Product | Sub-product |
|---|---|---|
| v1_2013_2017 | Payday loan | any (the product had none) |
| v2_2017_2023 | Payday loan, title loan, or personal loan | Payday loan |
| v3_2023_now | Payday loan, title loan, personal loan, or advance loan | Payday loan |

Original `product`, `sub_product` and `issue` values are always kept; `label_version` and `issue_group` are added next to them, never instead of them.
