# CFPB Complaint API — observed behaviour

**Tested on:** 17 November 2026
**Endpoint:** `https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/`
**Official parameter list:** https://cfpb.github.io/ccdb5-api/documentation/
**Data licence (from `_meta.license`):** CC0 (public domain)

## 1. Tests performed

| Test | Request (main parameters) | Observation |
|---|---|---|
| A — Structure | `date_received_min=2026-10-26&date_received_max=2026-10-27&size=3&no_aggs=true&sort=created_date_asc` | HTTP 200. Top-level keys: `took`, `timed_out`, `_shards`, `hits`, `_meta`. Records are in `hits.hits[*]._source`. |
| A — Total format | same | `hits.total` is an object: `{"value": 59984, "relation": "eq"}` (not a plain number). |
| A — Hit structure | same | Each hit has `_index`, `_id` (= complaint ID), `_score`, `_source`, `sort`. `sort` = `[epoch_milliseconds, "complaint_id"]`, e.g. `[1792972818000, "30990964"]`. |
| A — Field names | same | `_source` fields: `complaint_id`, `date_received`, `date_sent_to_company`, `product`, `sub_product`, `issue`, `sub_issue`, `company`, `company_public_response`, `company_response`, `timely`, `state`, `zip_code`, `submitted_via`, `tags`. No narrative field. |
| A — Types | same | `complaint_id` is a **string** (`"30990964"`). Dates are ISO-8601 UTC with milliseconds: `"2026-10-26T00:00:18.000Z"`. |
| A — Metadata | same | `_meta` contains `license`, `last_updated`, `last_indexed`, `total_record_count` (**19,225,276**), `is_data_stale` (false), `has_data_issue` (false), `break_points`. |
| B — Page size | `size=1000`, then `size=10000` | Both accepted: returned 1,000 and 10,000 records. |
| C — Product filter | add `&product=Credit%20card` | Works: total dropped from 59,984 to 721; all returned records have product `Credit card`. |
| D — End date inclusive? | min=max=2026-10-26 vs min=26/max=27 | min=max=26 returns 26,625 records; min=26/max=27 returns 59,984 and the last record is dated `2026-10-27T23:59:58Z`. **`date_received_max` is inclusive (whole day).** |
| E — Offset pagination | `size=3&frm=3`, `frm=6`, `page=2` | **All return the same first 3 records: `frm` and `page` are ignored for paging.** |
| E — Invalid offset | `size=100&frm=9950` | HTTP 400: `"frm is not zero or a multiple of size"`. |
| F — Cursor pagination | `search_after=<sort[0]>_<sort[1]>` of the last hit, e.g. `search_after=1792972840000_30990758` | **Works:** returns the next records (`30991185, 30990966, 30990967`), identical to records 4–6 of a single 12-record request. Format with a comma returns HTTP 424. |
| G — Values (whole database, via aggregations) | `size=1` (aggregations on) | See section 3. |

## 2. Volumes observed

| Scope | Records |
|---|---|
| Whole database (`_meta.total_record_count`) | 19,225,276 |
| One day, all products (26 Oct 2026) | 26,625 |
| Two days, all products (27–28 Oct 2026) | 67,074 |
| Two days, product `Credit card` (26–27 Oct 2026) | 721 |
| One month, product `Credit card` (Oct 2026) | 9,604 |
| Earliest record | December 2011 |

Products in the database (record counts, all years):

| Product | Records |
|---|---|
| Credit reporting or other personal consumer reports | 13,552,848 |
| Credit reporting, credit repair services, or other personal consumer reports | 2,163,769 |
| Debt collection | 1,238,943 |
| Mortgage | 467,094 |
| Checking or savings account | 413,002 |
| Credit card | 362,140 |
| Credit card or prepaid card | 206,356 |
| Money transfer, virtual currency, or money service | 198,634 |
| Credit reporting | 140,426 |
| Student loan | 136,542 |
| Vehicle loan or lease | 107,841 |
| Bank account or service | 86,198 |
| Payday loan, title loan, personal loan, or advance loan | 42,025 |
| Consumer Loan | 31,559 |
| Payday loan, title loan, or personal loan | 30,614 |
| Prepaid card | 24,348 |
| Debt or credit management | 10,981 |
| Payday loan | 5,528 |
| Money transfers | 5,354 |
| Other financial service | 1,056 |
| Virtual currency | 18 |

Credit reporting alone is about 82 % of all records.

## 3. Categorical values observed

| Field | Values (all-time counts) |
|---|---|
| `submitted_via` | Web 18,584,512 · Referral 281,046 · Phone 222,448 · Postal mail 111,203 · Fax 25,644 · Email 423 |
| `timely` | Yes 19,111,101 · No 114,175 |
| `company_response` | Closed with explanation 11,882,497 · Closed with non-monetary relief 6,397,203 · In progress 642,650 · Closed with monetary relief 225,797 · Untimely response 36,326 · Closed without relief 17,867 · Closed 17,611 · Closed with relief 5,304 |
| `tags` | Servicemember 531,397 · Older American 245,537 · "Older American, Servicemember" 60,629 · otherwise null |
| `company_public_response` | Mostly "Company has responded to the consumer and the CFPB and chooses not to provide a public response" (10.25 M); otherwise a short pre-set list or null |
| `zip_code` | 5-digit or 3-digit codes; null for 8 of 2,000 sampled records |

## 4. Consequences for our design

1. **Pagination must use `search_after`, not `frm`.** The collector reads `sort` of the last hit and sends `search_after=<epoch_ms>_<complaint_id>` for the next page, until a page returns fewer records than `size`.
2. **Page size:** `API_PAGE_SIZE=1000` (accepted; one page normally covers a whole day for our option).
3. **Daily windows use `date_received_min = date_received_max = day`** because the end date is inclusive; using `max = day + 1` would download every day twice.
4. **`hits.total` is an object** → read `hits.total.value`.
5. **API field names differ from CSV headers** (`company_response` vs "Company response to consumer", `timely` vs "Timely response?") → mapped to one canonical schema in `processing/schema.py`.
6. **`complaint_id` arrives as a string** → cast to integer in Spark; non-numeric IDs are rejected.
7. **Dates are UTC timestamps** → we keep the date part (`YYYY-MM-DD`) as `date_received`.
8. **`tags` is a comma-separated string**, not a list → split into an array in Spark.
9. **Product names changed over time.** Our option (payday lending) uses three different labels; see section 5. The scope is defined once in `config/scope_option11.json` and used by both the collector and Spark.
10. **Volume:** 19.2 M records in total, but only about 19,000 payday-loan records (about 10 new ones per day). The full archive is kept in the raw zone; Spark filters it to our scope.
11. **Freshness:** `_meta.last_indexed`, `is_data_stale` and `has_data_issue` can be stored with each run and shown on the dashboard.
12. **Publication delay** (complaints appear after the company responds or after 15 days) → 21-day lookback overlap, duplicates removed by Complaint ID.

13. **Date field used by the collector:** `date_received`. Records are requested by the date the CFPB received them. The 21-day overlap catches complaints published late. It does **not** catch revisions to older records (for example a company response changing months later). Those are reconciled by re-running the historical load from a new ZIP (for example once a month): the API version wins only when it is more recent, as documented in the deduplication rule.

## 5. Option 11 — payday lending (tested 20 November 2026)

### 5.1 How payday loans are labelled in the source

The CFPB moved payday loans between three product labels over time. The periods do not overlap, so together they give one continuous series.

| Label version | Product | Sub-product | First record | Last record | Records |
|---|---|---|---|---|---|
| v1_2013_2017 | Payday loan | (none) | 2013-11-06 | 2017-04-21 | 5,528 |
| v2_2017_2023 | Payday loan, title loan, or personal loan | Payday loan | 2017-04-24 | 2023-08-24 | 6,963 |
| v3_2023_now | Payday loan, title loan, personal loan, or advance loan | Payday loan | 2023-08-25 | today | 6,552 |
| **Total** | | | | | **19,043** |

Excluded on purpose: the other sub-products of the same parent products (installment loans 37,933, personal lines of credit 14,584, title loans 5,418, pawn loans 214, earned wage access 443, other advances 798, tax refund anticipation 64). They are not payday loans. The older "Consumer Loan" product has no payday sub-product.

### 5.2 API filter for the option

| Test | Request | Result |
|---|---|---|
| Sub-product filter with bullet | `product=Payday loan, title loan, or personal loan•Payday loan` (U+2022 between product and sub-product) | 6,963 — works |
| Same for the newest label | `product=Payday loan, title loan, personal loan, or advance loan•Payday loan` | 6,552 — works |
| `sub_product=Payday loan` parameter | | **Ignored** (returns all 19.3 M records) — do not use |
| All three labels together | repeat `product=` three times | 19,043 — equals the sum, so no overlap |

The collector sends the three `product` values from `config/scope_option11.json`. Spark applies the same rule again to the historical CSV, so both paths produce the same scope.

### 5.3 Volumes and sparsity

| Year | Records | | Year | Records |
|---|---|---|---|---|
| 2013 (from Nov) | 194 | | 2020 | 942 |
| 2014 | 1,703 | | 2021 | 737 |
| 2015 | 1,576 | | 2022 | 936 |
| 2016 | 1,562 | | 2023 | 1,208 |
| 2017 | 1,426 | | 2024 | 1,356 |
| 2018 | 1,396 | | 2025 | 2,277 |
| 2019 | 1,222 | | 2026 (to 20 Nov) | 2,508 |

Recent months: May 2026 179, Jun 218, Jul 282, Aug 265, Sep 249, Oct 240 (days 1–28 of each month). About 11 records per day. Monthly counts are small and noisy, so trends are reported **by quarter**, as the brief suggests for option 11.

### 5.4 Issue labels changed wording between versions

| v1 (2013–2017) | v2 / v3 (2017–today) |
|---|---|
| Charged fees or interest I didn't expect | Charged fees or interest you didn't expect |
| Can't contact lender | Can't contact lender or servicer |
| Received a loan I didn't apply for | Received a loan you didn't apply for |
| Can't stop charges to bank account | Can't stop withdrawals from your bank account |
| Charged bank acct wrong day or amt | Money was taken from your bank account on the wrong day or for the wrong amount |
| Payment to acct not credited | Loan payment wasn't credited to your account |
| Applied for loan/did not receive money | Was approved for a loan, but didn't receive the money |
| (no equivalent) | Struggling to pay your loan; Problem with the payoff process at the end of the loan |
| (no equivalent) | Seven credit-reporting issues (e.g. Incorrect information on your report) |

We keep the original `issue` text and add an `issue_group` from `config/issue_mapping.csv` (unexpected fees or interest, bank account withdrawals, repayment and payoff, contacting the lender, loan origination, credit reporting). Comparisons across 2017 use the groups; "Repayment and payoff" did not exist as a category before 2017, which is stated next to any trend that uses it.

### 5.5 Other values in scope

| Field | Values |
|---|---|
| Timely response | Yes 17,561 · No 1,482 (92.2 % timely) |
| Company response | Closed with explanation 17,207 · Closed with monetary relief 635 · Closed with non-monetary relief 493 · Untimely response 489 · Closed 196 · In progress 23 |
| Submitted via | Web 17,431 · Phone 945 · Referral 375 · Postal mail 169 · Fax 122 · Email 1 |
| Tags | Servicemember 1,724 · Older American 961 · both 256 (servicemember-tagged share 10.4 %, against 3.1 % in the whole database) |
| States | 57 codes; top: TX 2,468 · CA 2,196 · FL 1,331 · OH 1,005 · GA 762 |
| Top companies | Enova International 1,058 · Big Picture Loans 785 · CURO 772 · Populus Financial (Ace Cash Express) 683 · CNG Financial 613 · LDF Holdings 574 |

## 6. Historical download issues (20 November 2026)
- NiFi InvokeHTTP got "Access Denied" from the Akamai CDN with the default User-Agent; curl from the same container got HTTP 200. Fixed by setting Request User-Agent and an Accept header.
- Intermittent DNS failures inside Docker Desktop; fixed by pinning public DNS servers in docker-compose.yml.
- Collector container restarted in a loop because Windows port 8000 was taken; host port changed to 8001 (NiFi uses the internal address collector:8000).
- Result: archive 369,859,546 bytes, SHA-256 7bb5916e…3b69, 19,316,897 records (manifest full_2026-11-20).