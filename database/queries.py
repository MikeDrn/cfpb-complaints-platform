"""Meaningful aggregation queries (brief 6.4) and the before/after index benchmark.

python -m database.queries               -> runs the queries, writes docs/query_results.md
python -m database.queries --benchmark   -> compares collection scan vs. index, writes docs/index_benchmark.md
"""
import argparse
import json
import statistics
import time
from datetime import datetime, timezone

from pymongo import MongoClient

from config import settings
from database.schema import COMPLAINTS


def utc(y, m=1, d=1):
    return datetime(y, m, d, tzinfo=timezone.utc)


def pct(part, whole):
    return {"$cond": [{"$gt": [whole, 0]}, {"$multiply": [{"$divide": [part, whole]}, 100]}, None]}


# ---------------------------------------------------------------- the analytical queries
QUERIES = {
    "Q1 Quarterly complaints by issue group (2014 onwards)": {
        "question": "Q1 / Q2 — how complaints and their issue mix changed over time",
        "pipeline": [
            {"$match": {"date_received": {"$gte": utc(2014)}}},
            {"$group": {"_id": {"quarter": "$year_quarter", "issue_group": "$issue.group"}, "complaints": {"$sum": 1}}},
            {"$sort": {"_id.quarter": 1, "complaints": -1}},
        ],
    },
    "Q2 Issue-group share within each product-label period": {
        "question": "Q2 — has the mix of fees, withdrawals and repayment problems changed?",
        "pipeline": [
            {"$group": {"_id": {"period": "$product.label_version", "issue_group": "$issue.group"}, "n": {"$sum": 1}}},
            {"$group": {"_id": "$_id.period", "total": {"$sum": "$n"},
                        "groups": {"$push": {"issue_group": "$_id.issue_group", "n": "$n"}}}},
            {"$unwind": "$groups"},
            {"$project": {"_id": 0, "period": "$_id", "issue_group": "$groups.issue_group", "complaints": "$groups.n",
                          "total_in_period": "$total", "share_pct": pct("$groups.n", "$total")}},
            {"$sort": {"period": 1, "complaints": -1}},
        ],
    },
    "Q3 Company response and timely % by issue group": {
        "question": "Q3 — response categories and timeliness (valid Yes / valid Yes+No)",
        "pipeline": [
            {"$group": {"_id": "$issue.group", "complaints": {"$sum": 1},
                        "monetary_relief": {"$sum": {"$cond": [{"$eq": ["$company.response_to_consumer", "Closed with monetary relief"]}, 1, 0]}},
                        "non_monetary_relief": {"$sum": {"$cond": [{"$eq": ["$company.response_to_consumer", "Closed with non-monetary relief"]}, 1, 0]}},
                        "timely_yes": {"$sum": {"$cond": [{"$eq": ["$company.timely_response", True]}, 1, 0]}},
                        "timely_valid": {"$sum": {"$cond": [{"$in": ["$company.timely_response", [True, False]]}, 1, 0]}}}},
            {"$project": {"complaints": 1, "monetary_relief": 1, "non_monetary_relief": 1,
                          "monetary_relief_pct": pct("$monetary_relief", "$complaints"),
                          "timely_pct": pct("$timely_yes", "$timely_valid")}},
            {"$sort": {"complaints": -1}},
        ],
    },
    "Q4 Issue profile of the five states with most complaints (2023 onwards)": {
        "question": "Q4 — do leading states show different issue profiles? (shares, not population-adjusted)",
        "pipeline": [
            {"$match": {"location.state": {"$in": ["TX", "CA", "FL", "OH", "GA"]}, "date_received": {"$gte": utc(2023)}}},
            {"$group": {"_id": {"state": "$location.state", "issue_group": "$issue.group"}, "n": {"$sum": 1}}},
            {"$group": {"_id": "$_id.state", "total": {"$sum": "$n"},
                        "groups": {"$push": {"issue_group": "$_id.issue_group", "n": "$n"}}}},
            {"$unwind": "$groups"},
            {"$project": {"_id": 0, "state": "$_id", "issue_group": "$groups.issue_group", "complaints": "$groups.n",
                          "state_total": "$total", "share_pct": pct("$groups.n", "$total")}},
            {"$sort": {"state": 1, "complaints": -1}},
        ],
    },
    "Q5 Servicemember and Older American share by issue group": {
        "question": "Q5 — are tagged groups concentrated in particular issues?",
        "pipeline": [
            {"$group": {"_id": "$issue.group", "complaints": {"$sum": 1},
                        "servicemember": {"$sum": {"$cond": ["$is_servicemember", 1, 0]}},
                        "older_american": {"$sum": {"$cond": ["$is_older_american", 1, 0]}}}},
            {"$project": {"complaints": 1, "servicemember": 1, "older_american": 1,
                          "servicemember_pct": pct("$servicemember", "$complaints"),
                          "older_american_pct": pct("$older_american", "$complaints")}},
            {"$sort": {"complaints": -1}},
        ],
    },
    "Q6 Receipt-to-referral interval by year (valid intervals only)": {
        "question": "Common measure — days from CFPB receipt to referral (not resolution time)",
        "pipeline": [
            {"$group": {"_id": "$year", "complaints": {"$sum": 1},
                        "valid_intervals": {"$sum": {"$cond": [{"$eq": ["$lag_status", "ok"]}, 1, 0]}},
                        "avg_days": {"$avg": {"$cond": [{"$eq": ["$lag_status", "ok"]}, "$processing_lag_days", None]}},
                        "max_days": {"$max": {"$cond": [{"$eq": ["$lag_status", "ok"]}, "$processing_lag_days", None]}},
                        "negative_or_missing": {"$sum": {"$cond": [{"$ne": ["$lag_status", "ok"]}, 1, 0]}}}},
            {"$sort": {"_id": 1}},
        ],
    },
    "Q7 Companies' share of complaints since 2023 (not a quality ranking)": {
        "question": "Company comparison with denominators",
        "pipeline": [
            {"$match": {"date_received": {"$gte": utc(2023)}}},
            {"$group": {"_id": "$company.name", "complaints": {"$sum": 1}}},
            {"$group": {"_id": None, "total": {"$sum": "$complaints"},
                        "companies": {"$push": {"company": "$_id", "complaints": "$complaints"}}}},
            {"$unwind": "$companies"},
            {"$project": {"_id": 0, "company": "$companies.company", "complaints": "$companies.complaints",
                          "share_pct": pct("$companies.complaints", "$total")}},
            {"$sort": {"complaints": -1}}, {"$limit": 10},
        ],
    },
}

# ---------------------------------------------------------------- selective queries for the index benchmark
BENCHMARK = {
    "Ohio complaints since 2024, by quarter (state_date)": [
        {"$match": {"location.state": "OH", "date_received": {"$gte": utc(2024)}}},
        {"$group": {"_id": "$year_quarter", "n": {"$sum": 1}}}],
    "Repayment & payoff complaints in 2025-2026 (issue_group_date)": [
        {"$match": {"issue.group": "Repayment and payoff", "date_received": {"$gte": utc(2025)}}},
        {"$group": {"_id": "$company.response_to_consumer", "n": {"$sum": 1}}}],
    "One company over time (company_date)": [
        {"$match": {"company.name": "ENOVA INTERNATIONAL, INC.", "date_received": {"$gte": utc(2020)}}},
        {"$group": {"_id": "$year", "n": {"$sum": 1}}}],
    "Servicemember-tagged complaints since 2023 (tags_date, multikey)": [
        {"$match": {"tags": "Servicemember", "date_received": {"$gte": utc(2023)}}},
        {"$group": {"_id": "$issue.group", "n": {"$sum": 1}}}],
    "Last 90 days of complaints (date)": [
        {"$match": {"date_received": {"$gte": utc(2026, 8, 20)}}},
        {"$group": {"_id": "$submitted_via", "n": {"$sum": 1}}}],
}


def find_stat(obj, key):
    """Explain output differs between query engines; search the whole tree for a statistic."""
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            found = find_stat(v, key)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for v in obj:
            found = find_stat(v, key)
            if found is not None:
                return found
    return None


def find_stage(obj, names=("IXSCAN", "COUNT_SCAN", "DISTINCT_SCAN", "COLLSCAN")):
    if isinstance(obj, dict):
        if obj.get("stage") in names:
            return obj["stage"] + (f" ({obj['indexName']})" if obj.get("indexName") else "")
        for v in obj.values():
            s = find_stage(v, names)
            if s:
                return s
    elif isinstance(obj, list):
        for v in obj:
            s = find_stage(v, names)
            if s:
                return s
    return None


def measure(db, pipeline, hint=None, repeats=7):
    cmd = {"aggregate": COMPLAINTS, "pipeline": pipeline, "cursor": {}}
    if hint:
        cmd["hint"] = hint
    explain = db.command({"explain": cmd, "verbosity": "executionStats"})
    timings = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        list(db[COMPLAINTS].aggregate(pipeline, **({"hint": hint} if hint else {})))
        timings.append((time.perf_counter() - t0) * 1000)
    return {"plan": find_stage(explain) or "?", "docs_examined": find_stat(explain, "totalDocsExamined"),
            "keys_examined": find_stat(explain, "totalKeysExamined"),
            "returned_by_match": find_stat(explain, "nReturned"),
            "median_ms": round(statistics.median(timings), 2)}


def fmt(v):
    return f"{v:.1f}" if isinstance(v, float) else ("" if v is None else str(v))


def run_queries(db):
    lines = [f"# Query results — option {settings.OPTION_NUMBER}",
             f"Generated {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC by `database/queries.py` "
             f"on {db[COMPLAINTS].count_documents({}):,} documents.", ""]
    for name, q in QUERIES.items():
        rows = list(db[COMPLAINTS].aggregate(q["pipeline"], allowDiskUse=True))
        lines += [f"## {name}", f"*{q['question']}*", "", "```js",
                                    ("db.complaints.aggregate(" + json.dumps(q["pipeline"], default=lambda o: f"ISODate('{o:%Y-%m-%d}')") + ")")
                  .replace('"ISODate(', "ISODate(").replace(')"', ")"), "```", ""]
        if rows:
            flat = []
            for r in rows[:30]:
                d = {}
                for k, v in r.items():
                    if isinstance(v, dict):
                        d.update({f"{k}.{kk}": vv for kk, vv in v.items()})
                    else:
                        d[k] = v
                flat.append(d)
            cols = list(dict.fromkeys(k for d in flat for k in d))
            lines += ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
            lines += ["| " + " | ".join(fmt(d.get(c)) for c in cols) + " |" for d in flat]
            if len(rows) > 30:
                lines.append(f"\n… {len(rows) - 30} more rows")
        lines.append("")
    return lines


def run_benchmark(db):
    n = db[COMPLAINTS].count_documents({})
    lines = [f"# Index benchmark — before vs after indexing",
             f"Generated {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC on {n:,} documents. "
             "Same data and same query conditions; 'before' forces a collection scan with `hint: {$natural: 1}`, "
             "'after' lets MongoDB choose an index. Times are the median of 7 runs.", "",
             "| Query | Plan before | Docs examined before | ms before | Plan after | Keys examined | Docs examined after | ms after |",
             "|---|---|---|---|---|---|---|---|"]
    for name, pipeline in BENCHMARK.items():
        before = measure(db, pipeline, hint={"$natural": 1})
        after = measure(db, pipeline)
        lines.append(f"| {name} | {before['plan']} | {fmt(before['docs_examined'])} | {before['median_ms']} | "
                     f"{after['plan']} | {fmt(after['keys_examined'])} | {fmt(after['docs_examined'])} | {after['median_ms']} |")
        print(f"{name}: {before['docs_examined']} docs/{before['median_ms']} ms  ->  "
              f"{after['docs_examined']} docs/{after['median_ms']} ms ({after['plan']})")
    lines += ["", "**How to read it:** with an index, MongoDB examines only the documents that match the filter "
              "instead of all documents. With about 19,000 documents both versions are fast, so the clearest "
              "evidence is *documents examined*; the gap grows linearly with collection size. Each index also "
              "costs storage and slows every write, so we only index fields the dashboard filters on."]
    return lines


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark", action="store_true")
    args = parser.parse_args()
    db = MongoClient(settings.MONGO_URI)[settings.MONGO_DB]
    lines = run_benchmark(db) if args.benchmark else run_queries(db)
    out = "docs/index_benchmark.md" if args.benchmark else "docs/query_results.md"
    with open(out, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    print("\n".join(lines) if not args.benchmark else "")
    print(f"written {out}")


if __name__ == "__main__":
    main()
