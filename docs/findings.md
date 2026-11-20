# Findings — option 11: Payday lending
Generated 2026-11-20 22:40 UTC by `analytics/analysis.py` from the MongoDB serving layer (run `api_20261120T223848Z_3d0e3f`, quality status `pass`). All CSV outputs are in `analytics/outputs/`.

## Scope and metric definitions
- **Scope:** 19,043 distinct curated Complaint IDs about payday loans, received 2013-11-06 → 2026-11-18.
- **Complaints:** count of distinct curated Complaint IDs (never rows, never raw records).
- **Share:** complaints in a category ÷ complaints in the group it is compared within (stated each time).
- **Timely %:** valid Yes ÷ (valid Yes + valid No); 0 missing/invalid values reported separately.
- **Receipt-to-referral days:** date sent to company − date received, only for valid non-negative intervals (26 excluded). It is CFPB handling time, not company resolution time.
- **Partial periods:** 2013 and 2026 are partial years; 2026-Q4 is a partial quarter.

## Common measures
- Total complaints: **19,043**. Timely: **92.2 %** (17,561 of 19,043 valid).
- Receipt-to-referral: median **0** days, mean 5.1, 90th percentile 12 (n = 19,017).
- Leading issue group: **Unexpected fees or interest** (33.0 %).
- Channel: Web 91.5 %; state missing for 183 complaints.
- Freshness: latest complaint 2026-11-18; latest successful ingestion 2026-11-20T22:38:50Z; serving layer refreshed 2026-11-20 22:39:58.242000.

### Yearly counts
| year | complaints | coverage | change vs previous year (%) |
|---|---|---|---|
| 2013 | 194 | partial |  |
| 2014 | 1,703 | complete |  |
| 2015 | 1,576 | complete | -7.5 |
| 2016 | 1,562 | complete | -0.9 |
| 2017 | 1,426 | complete | -8.7 |
| 2018 | 1,396 | complete | -2.1 |
| 2019 | 1,222 | complete | -12.5 |
| 2020 | 942 | complete | -22.9 |
| 2021 | 737 | complete | -21.8 |
| 2022 | 936 | complete | 27.0 |
| 2023 | 1,208 | complete | 29.1 |
| 2024 | 1,356 | complete | 12.3 |
| 2025 | 2,277 | complete | 67.9 |
| 2026 | 2,508 | partial |  |

## Q1 — How have payday complaints changed per quarter?
Complete quarters averaged **266.7** complaints in 2019–2024 and **632.9** since 2025 (×2.37). The lowest complete quarter was 2020-Q4 (159), the highest 2026-Q3 (890).

*Caution:* counts describe published complaints, not the number of borrowers or loans; a rise can reflect more lending, more awareness of the CFPB or changes in how complaints are routed. Product labels changed in April 2017 and August 2023; the series is continuous because the three labels do not overlap.

## Q2 — Has the issue mix changed across the three label periods? *(demo question)*
| issue group (% of period) | v1_2013_2017 | v2_2017_2023 | v3_2023_now |
|---|---|---|---|
| Bank account withdrawals | 14.2 | 11.2 | 15.6 |
| Contacting the lender | 18.3 | 7.1 | 6.5 |
| Credit reporting | — | 7.5 | 6.5 |
| Loan origination | 16.2 | 14.8 | 13.6 |
| Repayment and payoff | 6.9 | 32.8 | 27.8 |
| Unexpected fees or interest | 44.5 | 26.7 | 30.0 |

— = the category did not exist in that period.

*Caution:* the April 2017 form added 'Struggling to pay your loan' and 'Problem with the payoff process', and seven credit-reporting issues. The jump in 'Repayment and payoff' and the appearance of 'Credit reporting' after 2017 are largely category changes, so comparisons across 2017 should focus on groups that existed in every period (unexpected fees, withdrawals, origination, contacting the lender).

## Q3 — Do company responses and timeliness differ by issue group?
| issue_group | complaints | timely_pct | monetary_relief_pct | monetary_relief |
|---|---|---|---|---|
| Unexpected fees or interest | 6,283 | 91.9 | 3.9 | 243 |
| Repayment and payoff | 4,484 | 93.4 | 2.5 | 113 |
| Loan origination | 2,813 | 92.4 | 1.5 | 43 |
| Bank account withdrawals | 2,589 | 92.7 | 7.3 | 188 |
| Contacting the lender | 1,928 | 87.9 | 2.2 | 43 |
| Credit reporting | 946 | 96.0 | 0.5 | 5 |

*Caution:* response categories are reported by companies; 'monetary relief' is rare, so differences of one or two points rest on small counts. A timely response is not a satisfactory one.

## Q4 — Do states with at least 300 complaints have different issue profiles?
`issue_mix_distance` = half the sum of absolute differences between a state's issue-group shares and the national shares (0 = identical mix, 1 = completely different).

| state | complaints | issue_mix_distance | most_over_represented | most_under_represented |
|---|---|---|---|---|
| WA | 389 | 0.184 | Unexpected fees or interest (+18.4 pts) | Loan origination (-5.3 pts) |
| NJ | 539 | 0.184 | Unexpected fees or interest (+18.4 pts) | Bank account withdrawals (-5.6 pts) |
| WI | 338 | 0.124 | Loan origination (+7.7 pts) | Unexpected fees or interest (-9.3 pts) |
| MD | 442 | 0.123 | Unexpected fees or interest (+12.3 pts) | Loan origination (-5.5 pts) |
| MO | 419 | 0.111 | Bank account withdrawals (+5.7 pts) | Unexpected fees or interest (-9.4 pts) |
| NC | 442 | 0.095 | Unexpected fees or interest (+9.5 pts) | Repayment and payoff (-2.5 pts) |
| GA | 762 | 0.091 | Repayment and payoff (+3.5 pts) | Contacting the lender (-4.2 pts) |
| AL | 349 | 0.091 | Repayment and payoff (+4.5 pts) | Unexpected fees or interest (-5.8 pts) |
| TX | 2,468 | 0.087 | Loan origination (+4.2 pts) | Unexpected fees or interest (-8.7 pts) |
| MI | 523 | 0.076 | Contacting the lender (+4.0 pts) | Repayment and payoff (-4.0 pts) |

*Caution:* shares avoid population size but still describe complaints, not how often borrowers have problems; payday lending is regulated differently across states, which affects how many loans exist at all.

## Q5 — Are Servicemember and Older American complaints over-represented? *(demo question)*
- Servicemember-tagged: **10.4 %** of payday complaints vs 3.08 % of all CFPB complaints.
- Older American-tagged: **6.4 %** vs 1.59 %.

| issue_group | complaints | servicemember_pct | older_american_pct |
|---|---|---|---|
| Unexpected fees or interest | 6,283 | 10.6 | 8.3 |
| Repayment and payoff | 4,484 | 10.6 | 5.5 |
| Loan origination | 2,813 | 11.2 | 6.0 |
| Bank account withdrawals | 2,589 | 10.4 | 5.9 |
| Contacting the lender | 1,928 | 8.6 | 5.0 |
| Credit reporting | 946 | 8.9 | 3.1 |

By label period:

| label_version | complaints | servicemember % | older american % |
|---|---|---|---|
| v1_2013_2017 | 5,528 | 11.0 | 7.5 |
| v2_2017_2023 | 6,963 | 11.4 | 5.7 |
| v3_2023_now | 6,552 | 8.8 | 6.2 |

*Caution:* tags rely on what the submitter reported. A higher share among complaints does not mean these consumers are more likely to have problems; it may reflect who uses payday loans or who files complaints.

## Implications for the intended user
- Monitor the 2025–2026 rise quarter by quarter, reading the latest quarter as partial.
- Fee-related complaints remain the largest stable category across all label periods.
- The Servicemember over-representation is a lead for further investigation with external data on payday borrowing among servicemembers, not a conclusion on its own.