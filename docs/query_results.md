# Query results — option 11
Generated 2026-11-20 20:13 UTC by `database/queries.py` on 19,043 documents.

## Q1 Quarterly complaints by issue group (2014 onwards)
*Q1 / Q2 — how complaints and their issue mix changed over time*

```js
db.complaints.aggregate([{"$match": {"date_received": {"$gte": ISODate('2014-01-01')}}}, {"$group": {"_id": {"quarter": "$year_quarter", "issue_group": "$issue.group"}, "complaints": {"$sum": 1}}}, {"$sort": {"_id.quarter": 1, "complaints": -1}}])
```

| _id.quarter | _id.issue_group | complaints |
|---|---|---|
| 2014-Q1 | Unexpected fees or interest | 219 |
| 2014-Q1 | Loan origination | 56 |
| 2014-Q1 | Bank account withdrawals | 51 |
| 2014-Q1 | Contacting the lender | 44 |
| 2014-Q1 | Repayment and payoff | 19 |
| 2014-Q2 | Unexpected fees or interest | 198 |
| 2014-Q2 | Loan origination | 55 |
| 2014-Q2 | Contacting the lender | 43 |
| 2014-Q2 | Bank account withdrawals | 36 |
| 2014-Q2 | Repayment and payoff | 18 |
| 2014-Q3 | Unexpected fees or interest | 218 |
| 2014-Q3 | Contacting the lender | 96 |
| 2014-Q3 | Loan origination | 86 |
| 2014-Q3 | Bank account withdrawals | 77 |
| 2014-Q3 | Repayment and payoff | 29 |
| 2014-Q4 | Unexpected fees or interest | 191 |
| 2014-Q4 | Contacting the lender | 92 |
| 2014-Q4 | Loan origination | 82 |
| 2014-Q4 | Bank account withdrawals | 50 |
| 2014-Q4 | Repayment and payoff | 43 |
| 2015-Q1 | Unexpected fees or interest | 192 |
| 2015-Q1 | Contacting the lender | 87 |
| 2015-Q1 | Loan origination | 80 |
| 2015-Q1 | Bank account withdrawals | 59 |
| 2015-Q1 | Repayment and payoff | 31 |
| 2015-Q2 | Unexpected fees or interest | 149 |
| 2015-Q2 | Loan origination | 72 |
| 2015-Q2 | Contacting the lender | 71 |
| 2015-Q2 | Bank account withdrawals | 56 |
| 2015-Q2 | Repayment and payoff | 32 |

… 269 more rows

## Q2 Issue-group share within each product-label period
*Q2 — has the mix of fees, withdrawals and repayment problems changed?*

```js
db.complaints.aggregate([{"$group": {"_id": {"period": "$product.label_version", "issue_group": "$issue.group"}, "n": {"$sum": 1}}}, {"$group": {"_id": "$_id.period", "total": {"$sum": "$n"}, "groups": {"$push": {"issue_group": "$_id.issue_group", "n": "$n"}}}}, {"$unwind": "$groups"}, {"$project": {"_id": 0, "period": "$_id", "issue_group": "$groups.issue_group", "complaints": "$groups.n", "total_in_period": "$total", "share_pct": {"$cond": [{"$gt": ["$total", 0]}, {"$multiply": [{"$divide": ["$groups.n", "$total"]}, 100]}, null]}}}, {"$sort": {"period": 1, "complaints": -1}}])
```

| period | issue_group | complaints | total_in_period | share_pct |
|---|---|---|---|---|
| v1_2013_2017 | Unexpected fees or interest | 2458 | 5528 | 44.5 |
| v1_2013_2017 | Contacting the lender | 1010 | 5528 | 18.3 |
| v1_2013_2017 | Loan origination | 896 | 5528 | 16.2 |
| v1_2013_2017 | Bank account withdrawals | 783 | 5528 | 14.2 |
| v1_2013_2017 | Repayment and payoff | 381 | 5528 | 6.9 |
| v2_2017_2023 | Repayment and payoff | 2283 | 6963 | 32.8 |
| v2_2017_2023 | Unexpected fees or interest | 1857 | 6963 | 26.7 |
| v2_2017_2023 | Loan origination | 1028 | 6963 | 14.8 |
| v2_2017_2023 | Bank account withdrawals | 782 | 6963 | 11.2 |
| v2_2017_2023 | Credit reporting | 520 | 6963 | 7.5 |
| v2_2017_2023 | Contacting the lender | 493 | 6963 | 7.1 |
| v3_2023_now | Unexpected fees or interest | 1968 | 6552 | 30.0 |
| v3_2023_now | Repayment and payoff | 1820 | 6552 | 27.8 |
| v3_2023_now | Bank account withdrawals | 1024 | 6552 | 15.6 |
| v3_2023_now | Loan origination | 889 | 6552 | 13.6 |
| v3_2023_now | Credit reporting | 426 | 6552 | 6.5 |
| v3_2023_now | Contacting the lender | 425 | 6552 | 6.5 |

## Q3 Company response and timely % by issue group
*Q3 — response categories and timeliness (valid Yes / valid Yes+No)*

```js
db.complaints.aggregate([{"$group": {"_id": "$issue.group", "complaints": {"$sum": 1}, "monetary_relief": {"$sum": {"$cond": [{"$eq": ["$company.response_to_consumer", "Closed with monetary relief"]}, 1, 0]}}, "non_monetary_relief": {"$sum": {"$cond": [{"$eq": ["$company.response_to_consumer", "Closed with non-monetary relief"]}, 1, 0]}}, "timely_yes": {"$sum": {"$cond": [{"$eq": ["$company.timely_response", true]}, 1, 0]}}, "timely_valid": {"$sum": {"$cond": [{"$in": ["$company.timely_response", [true, false]]}, 1, 0]}}}}, {"$project": {"complaints": 1, "monetary_relief": 1, "non_monetary_relief": 1, "monetary_relief_pct": {"$cond": [{"$gt": ["$complaints", 0]}, {"$multiply": [{"$divide": ["$monetary_relief", "$complaints"]}, 100]}, null]}, "timely_pct": {"$cond": [{"$gt": ["$timely_valid", 0]}, {"$multiply": [{"$divide": ["$timely_yes", "$timely_valid"]}, 100]}, null]}}}, {"$sort": {"complaints": -1}}])
```

| _id | complaints | monetary_relief | non_monetary_relief | monetary_relief_pct | timely_pct |
|---|---|---|---|---|---|
| Unexpected fees or interest | 6283 | 243 | 126 | 3.9 | 91.9 |
| Repayment and payoff | 4484 | 113 | 107 | 2.5 | 93.4 |
| Loan origination | 2813 | 43 | 108 | 1.5 | 92.4 |
| Bank account withdrawals | 2589 | 188 | 53 | 7.3 | 92.7 |
| Contacting the lender | 1928 | 43 | 72 | 2.2 | 87.9 |
| Credit reporting | 946 | 5 | 27 | 0.5 | 96.0 |

## Q4 Issue profile of the five states with most complaints (2023 onwards)
*Q4 — do leading states show different issue profiles? (shares, not population-adjusted)*

```js
db.complaints.aggregate([{"$match": {"location.state": {"$in": ["TX", "CA", "FL", "OH", "GA"]}, "date_received": {"$gte": ISODate('2023-01-01')}}}, {"$group": {"_id": {"state": "$location.state", "issue_group": "$issue.group"}, "n": {"$sum": 1}}}, {"$group": {"_id": "$_id.state", "total": {"$sum": "$n"}, "groups": {"$push": {"issue_group": "$_id.issue_group", "n": "$n"}}}}, {"$unwind": "$groups"}, {"$project": {"_id": 0, "state": "$_id", "issue_group": "$groups.issue_group", "complaints": "$groups.n", "state_total": "$total", "share_pct": {"$cond": [{"$gt": ["$total", 0]}, {"$multiply": [{"$divide": ["$groups.n", "$total"]}, 100]}, null]}}}, {"$sort": {"state": 1, "complaints": -1}}])
```

| state | issue_group | complaints | state_total | share_pct |
|---|---|---|---|---|
| CA | Unexpected fees or interest | 258 | 839 | 30.8 |
| CA | Repayment and payoff | 233 | 839 | 27.8 |
| CA | Bank account withdrawals | 119 | 839 | 14.2 |
| CA | Loan origination | 116 | 839 | 13.8 |
| CA | Contacting the lender | 77 | 839 | 9.2 |
| CA | Credit reporting | 36 | 839 | 4.3 |
| FL | Repayment and payoff | 179 | 621 | 28.8 |
| FL | Unexpected fees or interest | 165 | 621 | 26.6 |
| FL | Loan origination | 92 | 621 | 14.8 |
| FL | Bank account withdrawals | 83 | 621 | 13.4 |
| FL | Credit reporting | 51 | 621 | 8.2 |
| FL | Contacting the lender | 51 | 621 | 8.2 |
| GA | Repayment and payoff | 124 | 355 | 34.9 |
| GA | Unexpected fees or interest | 90 | 355 | 25.4 |
| GA | Loan origination | 45 | 355 | 12.7 |
| GA | Credit reporting | 43 | 355 | 12.1 |
| GA | Bank account withdrawals | 39 | 355 | 11.0 |
| GA | Contacting the lender | 14 | 355 | 3.9 |
| OH | Unexpected fees or interest | 140 | 344 | 40.7 |
| OH | Repayment and payoff | 77 | 344 | 22.4 |
| OH | Bank account withdrawals | 58 | 344 | 16.9 |
| OH | Loan origination | 30 | 344 | 8.7 |
| OH | Contacting the lender | 23 | 344 | 6.7 |
| OH | Credit reporting | 16 | 344 | 4.7 |
| TX | Repayment and payoff | 237 | 908 | 26.1 |
| TX | Unexpected fees or interest | 199 | 908 | 21.9 |
| TX | Loan origination | 183 | 908 | 20.2 |
| TX | Bank account withdrawals | 145 | 908 | 16.0 |
| TX | Credit reporting | 88 | 908 | 9.7 |
| TX | Contacting the lender | 56 | 908 | 6.2 |

## Q5 Servicemember and Older American share by issue group
*Q5 — are tagged groups concentrated in particular issues?*

```js
db.complaints.aggregate([{"$group": {"_id": "$issue.group", "complaints": {"$sum": 1}, "servicemember": {"$sum": {"$cond": ["$is_servicemember", 1, 0]}}, "older_american": {"$sum": {"$cond": ["$is_older_american", 1, 0]}}}}, {"$project": {"complaints": 1, "servicemember": 1, "older_american": 1, "servicemember_pct": {"$cond": [{"$gt": ["$complaints", 0]}, {"$multiply": [{"$divide": ["$servicemember", "$complaints"]}, 100]}, null]}, "older_american_pct": {"$cond": [{"$gt": ["$complaints", 0]}, {"$multiply": [{"$divide": ["$older_american", "$complaints"]}, 100]}, null]}}}, {"$sort": {"complaints": -1}}])
```

| _id | complaints | servicemember | older_american | servicemember_pct | older_american_pct |
|---|---|---|---|---|---|
| Unexpected fees or interest | 6283 | 668 | 522 | 10.6 | 8.3 |
| Repayment and payoff | 4484 | 477 | 247 | 10.6 | 5.5 |
| Loan origination | 2813 | 316 | 170 | 11.2 | 6.0 |
| Bank account withdrawals | 2589 | 269 | 152 | 10.4 | 5.9 |
| Contacting the lender | 1928 | 166 | 97 | 8.6 | 5.0 |
| Credit reporting | 946 | 84 | 29 | 8.9 | 3.1 |

## Q6 Receipt-to-referral interval by year (valid intervals only)
*Common measure — days from CFPB receipt to referral (not resolution time)*

```js
db.complaints.aggregate([{"$group": {"_id": "$year", "complaints": {"$sum": 1}, "valid_intervals": {"$sum": {"$cond": [{"$eq": ["$lag_status", "ok"]}, 1, 0]}}, "avg_days": {"$avg": {"$cond": [{"$eq": ["$lag_status", "ok"]}, "$processing_lag_days", null]}}, "max_days": {"$max": {"$cond": [{"$eq": ["$lag_status", "ok"]}, "$processing_lag_days", null]}}, "negative_or_missing": {"$sum": {"$cond": [{"$ne": ["$lag_status", "ok"]}, 1, 0]}}}}, {"$sort": {"_id": 1}}])
```

| _id | complaints | valid_intervals | avg_days | max_days | negative_or_missing |
|---|---|---|---|---|---|
| 2013 | 194 | 182 | 7.1 | 47 | 12 |
| 2014 | 1703 | 1689 | 6.0 | 497 | 14 |
| 2015 | 1576 | 1576 | 4.5 | 352 | 0 |
| 2016 | 1562 | 1562 | 9.6 | 406 | 0 |
| 2017 | 1426 | 1426 | 5.1 | 203 | 0 |
| 2018 | 1396 | 1396 | 4.6 | 361 | 0 |
| 2019 | 1222 | 1222 | 2.2 | 72 | 0 |
| 2020 | 942 | 942 | 2.6 | 213 | 0 |
| 2021 | 737 | 737 | 3.4 | 138 | 0 |
| 2022 | 936 | 936 | 4.6 | 162 | 0 |
| 2023 | 1208 | 1208 | 4.3 | 393 | 0 |
| 2024 | 1356 | 1356 | 2.4 | 161 | 0 |
| 2025 | 2277 | 2277 | 6.3 | 284 | 0 |
| 2026 | 2508 | 2508 | 5.7 | 179 | 0 |

## Q7 Companies' share of complaints since 2023 (not a quality ranking)
*Company comparison with denominators*

```js
db.complaints.aggregate([{"$match": {"date_received": {"$gte": ISODate('2023-01-01')}}}, {"$group": {"_id": "$company.name", "complaints": {"$sum": 1}}}, {"$group": {"_id": null, "total": {"$sum": "$complaints"}, "companies": {"$push": {"company": "$_id", "complaints": "$complaints"}}}}, {"$unwind": "$companies"}, {"$project": {"_id": 0, "company": "$companies.company", "complaints": "$companies.complaints", "share_pct": {"$cond": [{"$gt": ["$total", 0]}, {"$multiply": [{"$divide": ["$companies.complaints", "$total"]}, 100]}, null]}}}, {"$sort": {"complaints": -1}}, {"$limit": 10}])
```

| company | complaints | share_pct |
|---|---|---|
| CCF Intermediate Holdings LLC | 418 | 5.7 |
| Dave Operating, LLC | 307 | 4.2 |
| ENOVA INTERNATIONAL, INC. | 280 | 3.8 |
| WLCC | 268 | 3.6 |
| Rosebud Economic Development Corporation | 227 | 3.1 |
| Uprova Credit, LLC | 212 | 2.9 |
| Wolf River Development Company | 185 | 2.5 |
| MoneyLion Inc. | 184 | 2.5 |
| Ningo Lending LLC dba Spotloan | 178 | 2.4 |
| LDF Holdings, LLC | 162 | 2.2 |
