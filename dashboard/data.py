"""Data access for the dashboard: every view is a MongoDB aggregation on the serving layer.
Pure functions (no Streamlit) so they can be unit-tested."""
from datetime import date, datetime, time, timedelta, timezone

import pandas as pd
from pymongo.errors import PyMongoError

NOT_PROVIDED = "Not provided"
ISSUE_GROUPS = ["Unexpected fees or interest", "Repayment and payoff", "Bank account withdrawals",
                "Loan origination", "Contacting the lender", "Credit reporting", "Other / unmapped"]
LABEL_PERIODS = {"v1_2013_2017": "2013–2017 · 'Payday loan' product",
                 "v2_2017_2023": "2017–2023 · 'Payday loan, title loan, or personal loan'",
                 "v3_2023_now": "2023–now · '…personal loan, or advance loan'"}
LABEL_CHANGES = [("2017-Q2", "Product & issue labels changed (Apr 2017)"),
                 ("2023-Q3", "Product label changed (Aug 2023)")]


class DataUnavailable(RuntimeError):
    """Raised when MongoDB cannot be reached; the app shows a friendly message."""


class InvalidFilter(ValueError):
    pass


def validate_dates(start, end):
    if start is None or end is None:
        raise InvalidFilter("Choose both a start date and an end date.")
    if start > end:
        raise InvalidFilter("The start date is after the end date.")


def build_match(start, end, issue_groups=(), states=(), companies=(), channels=(), periods=()):
    validate_dates(start, end)
    match = {"date_received": {"$gte": datetime.combine(start, time.min, tzinfo=timezone.utc),
                               "$lt": datetime.combine(end + timedelta(days=1), time.min, tzinfo=timezone.utc)}}
    for field, values in (("issue.group", issue_groups), ("location.state", states),
                          ("company.name", companies), ("submitted_via", channels),
                          ("product.label_version", periods)):
        if values:
            vals = [None if v == NOT_PROVIDED else v for v in values]
            match[field] = {"$in": vals}
    return match


def _run(db, pipeline):
    try:
        return pd.DataFrame(list(db.complaints.aggregate(pipeline, allowDiskUse=True)))
    except PyMongoError as exc:
        raise DataUnavailable(str(exc)) from exc


def _labelled(df, col):
    if not df.empty and col in df:
        df[col] = df[col].fillna(NOT_PROVIDED)
    return df


def meta(db):
    try:
        return db.meta.find_one({"_id": "freshness"}) or {}, db.meta.find_one({"_id": "quality_latest"}) or {}
    except PyMongoError as exc:
        raise DataUnavailable(str(exc)) from exc


def options(db):
    try:
        return {
            "states": sorted(s for s in db.complaints.distinct("location.state") if s) + [NOT_PROVIDED],
            "channels": sorted(c for c in db.complaints.distinct("submitted_via") if c),
            "companies": [d["_id"] for d in db.complaints.aggregate([
                {"$group": {"_id": "$company.name", "n": {"$sum": 1}}}, {"$sort": {"n": -1}}, {"$limit": 40}])
                if d["_id"]],
            "issue_groups": [g for g in ISSUE_GROUPS if g in set(db.complaints.distinct("issue.group"))],
        }
    except PyMongoError as exc:
        raise DataUnavailable(str(exc)) from exc


def headline(db, match):
    df = _run(db, [{"$match": match}, {"$group": {
        "_id": None, "complaints": {"$sum": 1},
        "timely_yes": {"$sum": {"$cond": [{"$eq": ["$company.timely_response", True]}, 1, 0]}},
        "timely_no": {"$sum": {"$cond": [{"$eq": ["$company.timely_response", False]}, 1, 0]}},
        "lag_sum": {"$sum": {"$cond": [{"$eq": ["$lag_status", "ok"]}, "$processing_lag_days", 0]}},
        "lag_valid": {"$sum": {"$cond": [{"$eq": ["$lag_status", "ok"]}, 1, 0]}},
        "servicemember": {"$sum": {"$cond": ["$is_servicemember", 1, 0]}},
        "older_american": {"$sum": {"$cond": ["$is_older_american", 1, 0]}}}}])
    if df.empty or int(df.iloc[0]["complaints"]) == 0:
        return None
    r = df.iloc[0].to_dict()
    valid = r["timely_yes"] + r["timely_no"]
    r["timely_pct"] = 100 * r["timely_yes"] / valid if valid else None
    r["timely_invalid_or_missing"] = r["complaints"] - valid
    r["avg_lag"] = r["lag_sum"] / r["lag_valid"] if r["lag_valid"] else None
    r["lag_missing_or_invalid"] = r["complaints"] - r["lag_valid"]
    return r


def quarterly(db, match, by=None):
    key = {"q": "$year_quarter"}
    if by:
        key["k"] = by
    df = _run(db, [{"$match": match}, {"$group": {"_id": key, "complaints": {"$sum": 1}}}])
    if df.empty:
        return df
    out = pd.json_normalize(df.to_dict("records")).rename(columns={"_id.q": "quarter", "_id.k": "group"})
    return out.sort_values("quarter").reset_index(drop=True)


def yearly(db, match):
    df = _run(db, [{"$match": match}, {"$group": {"_id": "$year", "complaints": {"$sum": 1}}}, {"$sort": {"_id": 1}}])
    return df.rename(columns={"_id": "year"})


def share_by(db, match, field, top=None):
    pipeline = [{"$match": match}, {"$group": {"_id": f"${field}", "complaints": {"$sum": 1}}},
                {"$sort": {"complaints": -1}}]
    df = _labelled(_run(db, pipeline).rename(columns={"_id": "value"}), "value")
    if df.empty:
        return df
    df["share_pct"] = 100 * df["complaints"] / df["complaints"].sum()
    return df.head(top) if top else df


def mix(db, match, outer, inner):
    """Share of `inner` within each value of `outer` (e.g. issue group within each label period)."""
    df = _run(db, [{"$match": match}, {"$group": {"_id": {"o": f"${outer}", "i": f"${inner}"}, "complaints": {"$sum": 1}}}])
    if df.empty:
        return df
    df = pd.json_normalize(df.to_dict("records")).rename(columns={"_id.o": "outer", "_id.i": "inner"})
    df[["outer", "inner"]] = df[["outer", "inner"]].fillna(NOT_PROVIDED)
    df["outer_total"] = df.groupby("outer")["complaints"].transform("sum")
    df["share_pct"] = 100 * df["complaints"] / df["outer_total"]
    return df


def timely_by_issue(db, match):
    df = _run(db, [{"$match": match}, {"$group": {
        "_id": "$issue.group", "complaints": {"$sum": 1},
        "yes": {"$sum": {"$cond": [{"$eq": ["$company.timely_response", True]}, 1, 0]}},
        "no": {"$sum": {"$cond": [{"$eq": ["$company.timely_response", False]}, 1, 0]}},
        "monetary": {"$sum": {"$cond": [{"$eq": ["$company.response_to_consumer", "Closed with monetary relief"]}, 1, 0]}}}}])
    if df.empty:
        return df
    df = df.rename(columns={"_id": "issue_group"})
    valid = df["yes"] + df["no"]
    df["timely_pct"] = (100 * df["yes"] / valid).where(valid > 0)
    df["monetary_relief_pct"] = 100 * df["monetary"] / df["complaints"]
    return df.sort_values("complaints", ascending=False)


def tags_by_issue(db, match):
    df = _run(db, [{"$match": match}, {"$group": {
        "_id": "$issue.group", "complaints": {"$sum": 1},
        "servicemember": {"$sum": {"$cond": ["$is_servicemember", 1, 0]}},
        "older_american": {"$sum": {"$cond": ["$is_older_american", 1, 0]}}}}])
    if df.empty:
        return df
    df = df.rename(columns={"_id": "issue_group"})
    df["servicemember_pct"] = 100 * df["servicemember"] / df["complaints"]
    df["older_american_pct"] = 100 * df["older_american"] / df["complaints"]
    return df.sort_values("complaints", ascending=False)


def quarter_end(q):
    y, n = int(q[:4]), int(q[-1])
    return date(y, 12, 31) if n == 4 else date(y, 3 * n + 1, 1) - timedelta(days=1)


def partial_quarters(quarters, latest_date, period_start, period_end):
    """Quarters not fully covered by the data or the selected period are labelled partial."""
    partial = set()
    for q in quarters:
        y, n = int(q[:4]), int(q[-1])
        q_start, q_end = date(y, 3 * n - 2, 1), quarter_end(q)
        if (latest_date and q_end > latest_date) or q_start < period_start or q_end > period_end:
            partial.add(q)
    return partial


def connect(uri, db_name, timeout_ms=3000):
    """Returns the database or raises DataUnavailable (database stopped, wrong password, …)."""
    from pymongo import MongoClient
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=timeout_ms)
        client.admin.command("ping")
        return client[db_name]
    except PyMongoError as exc:
        raise DataUnavailable(str(exc)) from exc
