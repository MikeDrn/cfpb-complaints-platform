# Index benchmark — before vs after indexing
Generated 2026-11-20 20:08 UTC on 19,043 documents. Same data and same query conditions; 'before' forces a collection scan with `hint: {$natural: 1}`, 'after' lets MongoDB choose an index. Times are the median of 7 runs.

| Query | Plan before | Docs examined before | ms before | Plan after | Keys examined | Docs examined after | ms after |
|---|---|---|---|---|---|---|---|
| Ohio complaints since 2024, by quarter (state_date) | COLLSCAN | 19043 | 19.52 | IXSCAN (state_date) | 292 | 292 | 2.18 |
| Repayment & payoff complaints in 2025-2026 (issue_group_date) | COLLSCAN | 19043 | 20.19 | IXSCAN (issue_group_date) | 1299 | 1299 | 6.21 |
| One company over time (company_date) | COLLSCAN | 19043 | 15.8 | IXSCAN (company_date) | 423 | 423 | 2.62 |
| Servicemember-tagged complaints since 2023 (tags_date, multikey) | COLLSCAN | 19043 | 14.49 | IXSCAN (tags_date) | 668 | 668 | 4.49 |
| Last 90 days of complaints (date) | COLLSCAN | 19043 | 10.74 | IXSCAN (date) | 731 | 731 | 1.82 |

**How to read it:** with an index, MongoDB examines only the documents that match the filter instead of all documents. With about 19,000 documents both versions are fast, so the clearest evidence is *documents examined*; the gap grows linearly with collection size. Each index also costs storage and slows every write, so we only index fields the dashboard filters on.