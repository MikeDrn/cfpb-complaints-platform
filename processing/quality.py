"""Data-quality report and gate. Thresholds live in config/quality_thresholds.json."""
import json
from datetime import date

from pyspark.sql import functions as F

from processing.schema import CANONICAL_COLUMNS
from processing.transform import FLAG_NAMES

# Fields that do not apply to every record by design (brief 6.3: distinguish missing from not applicable)
NOT_APPLICABLE_RULES = {
    "sub_product": "label_version != 'v1_2013_2017'",   # the 2013-2017 'Payday loan' product had no sub-products
}


class QualityGateError(RuntimeError):
    pass


def load_thresholds(path="config/quality_thresholds.json"):
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def completeness(valid, n):
    if not n:
        return {}, {}
    row = valid.agg(*[F.count(F.col(c)).alias(c) for c in CANONICAL_COLUMNS]).first().asDict()
    overall = {c: round(100.0 * row[c] / n, 2) for c in CANONICAL_COLUMNS}
    applicable = {}
    for col, condition in NOT_APPLICABLE_RULES.items():
        sub = valid.filter(condition)
        m = sub.count()
        applicable[col] = {
            "applies_to_records": m,
            "not_applicable_records": n - m,
            "pct_filled_where_applicable": round(100.0 * sub.filter(F.col(col).isNotNull()).count() / m, 2) if m else None,
        }
    return overall, applicable


def build_report(run_id, mode, counts, dup_stats, valid, curated, rejected, last_ingestion):
    valid_n, curated_n = counts["valid"], counts["curated"]
    distinct_curated = curated.select("complaint_id").distinct().count()
    overall, applicable = completeness(valid, valid_n)
    flags = {f: 0 for f in FLAG_NAMES}
    if valid_n:
        flags = valid.agg(*[F.sum(F.array_contains("quality_flags", f).cast("int")).alias(f)
                            for f in FLAG_NAMES]).first().asDict()
    reasons = {r["reject_reason"]: r["count"] for r in rejected.groupBy("reject_reason").count().collect()}
    examples = [r.asDict() for r in rejected.select("reject_reason", "complaint_id_raw", "date_received_raw",
                                                     "_raw_object").limit(5).collect()]
    unmapped = [r["issue"] for r in valid.filter(F.array_contains("quality_flags", "unmapped_issue"))
                .select("issue").distinct().limit(20).collect()]
    labels = {r["label_version"]: r["count"] for r in curated.groupBy("label_version").count().collect()}
    max_date = curated.agg(F.max("date_received")).first()[0] if curated_n else None
    return {
        "run_id": run_id,
        "mode": mode,
        "reconciliation": {
            **counts,
            "check_raw_equals_filtered_plus_in_scope": counts["raw_rows"] == counts["filtered_out"] + counts["in_scope"],
            "check_in_scope_equals_valid_plus_rejected": counts["in_scope"] == counts["valid_new"] + counts["rejected"],
            "check_valid_equals_curated_plus_duplicates_removed":
                counts["valid"] == counts["curated"] + counts["duplicates_removed"],
        },
        "uniqueness": {**dup_stats, "duplicates_after": curated_n - distinct_curated},
        "completeness_pct": overall,
        "completeness_where_applicable": applicable,
        "validity_flags": {k: int(v or 0) for k, v in flags.items()},
        "rejected_reasons": reasons,
        "rejected_examples": examples,
        "consistency": {"records_by_label_version": labels, "unmapped_issues": unmapped},
        "freshness": {
            "latest_date_received": max_date.isoformat() if max_date else None,
            "days_behind_today": (date.today() - max_date).days if max_date else None,
            "latest_successful_ingestion": last_ingestion,
        },
    }


def evaluate(report, t):
    checks = []

    def add(name, value, threshold, ok, severity="fail"):
        checks.append({"check": name, "value": value, "threshold": threshold,
                       "status": "pass" if ok else severity})

    rec = report["reconciliation"]
    in_scope = rec["in_scope"] or 1
    rej_pct = round(100.0 * rec["rejected"] / in_scope, 3)
    add("rejected_pct", rej_pct, f"<= {t['max_rejected_pct']}", rej_pct <= t["max_rejected_pct"])
    dup_after = report["uniqueness"]["duplicates_after"]
    add("curated_ids_unique", dup_after, "= 0", dup_after == 0)
    for name in ("check_raw_equals_filtered_plus_in_scope", "check_in_scope_equals_valid_plus_rejected",
                 "check_valid_equals_curated_plus_duplicates_removed"):
        add(name, rec[name], "true", rec[name] is True)
    if rec.get("expected_raw_rows") is not None:
        add("raw_rows_match_manifest", rec["raw_rows"], rec["expected_raw_rows"],
            rec["raw_rows"] == rec["expected_raw_rows"])
    for field, minimum in t["required_completeness_pct"].items():
        value = report["completeness_pct"].get(field, 0.0)
        add(f"completeness_{field}", value, f">= {minimum}", value >= minimum)
    for field, minimum in t["warn_completeness_pct"].items():
        value = report["completeness_pct"].get(field, 0.0)
        add(f"completeness_{field}", value, f">= {minimum}", value >= minimum, severity="warn")
    sp = report["completeness_where_applicable"].get("sub_product", {}).get("pct_filled_where_applicable")
    if sp is not None:
        add("completeness_sub_product_where_applicable", sp, ">= 100", sp >= 100)
    for flag, maximum in t["max_flag_pct_warn"].items():
        pct = round(100.0 * report["validity_flags"].get(flag, 0) / in_scope, 3)
        add(f"flag_{flag}_pct", pct, f"<= {maximum}", pct <= maximum, severity="warn")
    days = report["freshness"]["days_behind_today"]
    add("freshness_days", days, f"<= {t['max_freshness_days']}",
        days is not None and days <= t["max_freshness_days"], severity="warn")
    statuses = {c["status"] for c in checks}
    overall = "fail" if "fail" in statuses else "warn" if "warn" in statuses else "pass"
    return checks, overall
