# Project charter — Public Banking Complaints Data Platform

**Course:** 020IABDM2 Big Data Frameworks · **Coordinator:** Dr. Yehia Taher
**Project option:** 11 — Payday lending (payday loan sub-product)
**Date:** 20 November 2026

## 1. Users

Policy analysts and operations managers at a fictional public-interest organisation that monitors consumer financial complaints. They need to understand payday-loan complaints without reading code.

## 2. Project question

How can a small data engineering team turn public complaint records about payday loans into useful evidence, while making clear what those records can and cannot tell us?

## 3. Scope

- **Source:** CFPB Consumer Complaint Database — official full CSV ZIP (historical load, all products kept in the raw zone) and the complaint API (scheduled every 24 hours, filtered to our option).
- **Option 11 filter:** the stand-alone "Payday loan" product (2013–2017) plus the "Payday loan" sub-product of "Payday loan, title loan, or personal loan" (2017–2023) and "Payday loan, title loan, personal loan, or advance loan" (2023–today). Defined in `config/scope_option11.json`.
- **Size:** about 19,000 complaints from November 2013 to today, about 11 new complaints per day.
- **Reporting period:** 2014-Q1 to the last complete quarter (2013 has only two months; the current quarter is labelled partial).
- **Trend level:** quarters, because monthly counts are sparse.
- **Out of scope:** title loans, installment loans and other sub-products of the same parent products; narratives (removed by the CFPB); streaming, prediction and NLP.

## 4. Platform

Python collector + Apache NiFi schedule → MinIO raw zone → Apache Spark (validation, scope filter, label harmonisation, deduplication, quality gate) → curated Parquet → MongoDB serving layer → Streamlit dashboard, all started with Docker Compose.

## 5. Five analytical questions

| # | Question | Measures | Caution placed next to the result |
|---|---|---|---|
| Q1 | How have payday-loan complaints changed per quarter since 2014, and how does the 2025–2026 level compare with earlier years? | Quarterly counts, year-on-year change of complete years | Counts reflect published complaints, not the number of borrowers or loans; the latest quarter is partial; label changes in 2017 and 2023 are marked on the chart |
| Q2 *(demo)* | What share of complaints concern unexpected fees or interest, bank-account withdrawals and repayment problems, and has this mix changed across the three label periods? | Counts and % per `issue_group` and `label_version` | Issue wording changed in 2017; "Repayment and payoff" has no pre-2017 equivalent |
| Q3 | How do company responses and the timely-response percentage differ by issue group? | Response distribution (%), timely % = valid Yes / (valid Yes + No) | Not a measure of satisfaction or resolution; monetary relief is rare (about 3 %), so small groups are flagged |
| Q4 | Do the states with the most payday-loan complaints show different issue profiles? | Issue-group shares per state for states with at least 300 complaints, compared with the national mix | Counts are not adjusted for population or for how many people use payday loans in each state |
| Q5 *(demo)* | Are complaints tagged Servicemember or Older American a larger share of payday-loan complaints than of all CFPB complaints, and which issue groups do they concentrate in? | Tagged share in scope vs. in the whole database; tagged share by issue group | Tags depend on what submitters report; a higher share does not show that these groups are more likely to have a problem |

Q2 and Q5 are the two pre-declared questions for the live demonstration.

## 6. Success criteria

- The historical load and an incremental run can be reproduced by another team member from an empty environment.
- Replaying the same date window leaves curated counts and dashboard measures unchanged.
- Every run produces run metadata and a quality report with pass/warn/fail checks.
- Records are reconciled at each stage: raw → filtered out → rejected → deduplicated → curated → loaded into MongoDB.
- Every dashboard figure can be traced to its filter, its source records and its check.

## 7. Risks and mitigations

| Risk | Mitigation |
|---|---|
| Product and issue labels changed over time | Three scope rules + issue mapping, original labels kept, `label_version` field, label changes marked on charts |
| Small and sparse counts | Quarterly trends, minimum group sizes before showing percentages, counts shown next to every percentage |
| API behaviour differs from documentation (offset paging and `sub_product` parameter ignored) | Verified 17 and 20 Nov 2026; cursor pagination and `Product•Sub-product` filter; documented in `api_notes.md` |
| Container images disappear (MinIO removed from Docker Hub, Sept 2026) | Chainguard MinIO build, pinned and documented |
| Large full archive (19.2 M records) on laptops | Raw zone keeps it untouched; Spark filters early to about 19,000 records |
| Late publication and later revisions | 21-day overlap with deduplication; periodic reload from a new ZIP for older revisions |
| Misreading counts as rankings | Percentages with denominators, no company ranking, limitations visible on the dashboard |

## 8. Team roles

| Role | Member |
|---|---|
| Data acquisition lead | Anna-Maria Lahoud and Mike Deryene |
| Data engineering lead | Mike Deryene and Anna-Maria Lahoud |
| Database lead | Anna-Maria Lahoud and Mike Deryene |
| Analytics lead | Mike Deryene and Anna-Maria Lahoud |
| Visualisation and delivery lead | Anna-Maria Lahoud and Mike Deryene |
