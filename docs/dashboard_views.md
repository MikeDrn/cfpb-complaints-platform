# Dashboard — views, definitions and checks

App: `dashboard/app.py` (http://localhost:8501) · data access: `dashboard/data.py` (MongoDB aggregations only, no individual complaint records).

## Filters
Date received (range) · Issue group · State (incl. "Not provided") · Company (40 with most complaints) · Submitted via · Product-label period. Empty filter = all values. Every view and headline number uses the same filters.

## Header (always visible)
Selected option and period; latest complaint date; last serving-layer refresh; quality status of the last run; whether curated and MongoDB counts reconcile; four headline measures with definitions in their help icons.

## Views

| Tab | Content | Question | Definitions and cautions shown next to it |
|---|---|---|---|
| Overview & trends | Quarterly line (partial quarters as hollow markers, label changes as dotted lines); yearly table with year-on-year change only between complete years | Q1 | Quarters because monthly counts are small; latest quarter still filling |
| Issues | Issue-group counts and shares; 100 % stacked issue mix per label period; original CFPB issues and sub-issues | Q2 (demo) | 2017 wording change and new categories; sub-issues absent by design |
| Geography, channel & companies | State map (counts) + missing-state count; channel shares; state issue-mix table vs national (min. size slider); company shares (top 10) | Q4 | Not population-adjusted; not a company quality ranking |
| Company responses | Response categories; timely % by issue group; monetary relief % by issue group | Q3 | Timely = valid Yes ÷ valid Yes+No, invalid excluded; not satisfaction |
| Servicemembers & older consumers | Tagged shares vs all-CFPB baseline; tagged share per issue group | Q5 (demo) | Tags are self-reported; over-representation ≠ higher risk |
| Data quality & limitations | Freshness, reconciliation, completeness, all quality checks, plain-language limitations | — | — |

## Accessibility and design
Fixed colour per issue group (validated colour-blind-safe palette), one blue for single-series charts, one-hue ramp for ordered label periods and the map, values written on or beside bars, hover tooltips with counts and denominators, legend outside the plot, no dual axes.

## Error handling (tested in `tests/test_dashboard.py`)
| Situation | Behaviour |
|---|---|
| Database unavailable | Clear message, no crash |
| Start date after end date / incomplete range | Warning asking to fix the filter |
| Filters with no matching complaints | Warning suggesting to widen filters |
| Missing category values | Shown as "Not provided" and selectable |
