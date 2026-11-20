"""Profile the raw full-load CSV and the option scope. Writes docs/profiling_<ingest_date>.md.

docker compose run --rm jobs python -m processing.profile_raw --ingest-date YYYY-MM-DD
"""
import argparse
import os
from datetime import datetime, timezone
from functools import reduce

from pyspark.sql import functions as F

from config import settings
from ingestion.storage import get_json
from processing.schema import explicit_string_schema, validate_header
from processing.spark_session import get_spark


def scope_condition():
    """Same rule Spark processing will use: product match, plus sub-product when the filter has one."""
    conds = []
    for f in settings.SCOPE_FILTERS:
        c = F.col("Product") == f["product"]
        if f["sub_product"]:
            c = c & (F.col("Sub-product") == f["sub_product"])
        conds.append(c)
    return reduce(lambda a, b: a | b, conds)


def label_version():
    expr = None
    for f in settings.SCOPE_FILTERS:
        c = F.col("Product") == f["product"]
        if f["sub_product"]:
            c = c & (F.col("Sub-product") == f["sub_product"])
        expr = F.when(c, f["label_version"]) if expr is None else expr.when(c, f["label_version"])
    return expr


def md_table(rows, headers):
    out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    out += ["| " + " | ".join("" if v is None else str(v) for v in r) + " |" for r in rows]
    return "\n".join(out)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest-date", required=True)
    args = parser.parse_args()

    prefix = f"cfpb/full_load/ingest_date={args.ingest_date}"
    manifest = get_json(settings.BUCKET_RAW, f"{prefix}/manifest.json")
    if manifest is None:
        raise SystemExit(f"No manifest at raw/{prefix}. Run the historical load first.")

    missing, unexpected = validate_header(manifest["header"])
    if missing:
        raise SystemExit(f"Source is missing required columns: {missing}")

    spark = get_spark("cfpb-profile")
    df = (spark.read.schema(explicit_string_schema(manifest["header"]))
          .option("header", True).option("multiLine", True).option("escape", '"')
          .option("mode", "PERMISSIVE")
          .csv(f"s3a://{settings.BUCKET_RAW}/{prefix}/complaints.csv"))

    total = df.count()
    date_rx = F.to_date(F.substring("Date received", 1, 10), "yyyy-MM-dd")
    overall = df.select(F.min(date_rx).alias("min"), F.max(date_rx).alias("max"),
                        F.sum(date_rx.isNull().cast("int")).alias("bad_dates")).first()

    scope = df.filter(scope_condition()).withColumn("label_version", label_version()).cache()
    scope_n = scope.count()
    dated = scope.withColumn("d", F.to_date(F.substring("Date received", 1, 10), "yyyy-MM-dd"))

    by_version = (dated.groupBy("label_version").agg(F.count("*").alias("n"), F.min("d").alias("from"),
                                                     F.max("d").alias("to")).orderBy("label_version").collect())
    by_year = dated.groupBy(F.year("d").alias("year")).count().orderBy("year").collect()
    by_quarter = (dated.groupBy(F.year("d").alias("y"), F.quarter("d").alias("q")).count()
                  .orderBy("y", "q").collect())
    issues = dated.groupBy("label_version", "Issue").count().orderBy("label_version", F.desc("count")).collect()
    completeness = scope.agg(*[F.round(100 * F.count(F.col(c)) / F.lit(max(scope_n, 1)), 1).alias(c)
                               for c in scope.columns if c != "label_version"]).first().asDict()
    dup_ids = scope.groupBy("Complaint ID").count().filter("count > 1").count()
    timely = scope.groupBy("Timely response?").count().collect()
    channel = scope.groupBy("Submitted via").count().orderBy(F.desc("count")).collect()
    states_missing = scope.filter(F.col("State").isNull()).count()

    lines = [
        f"# Raw data profile — full load {args.ingest_date}",
        f"Generated {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC by `processing/profile_raw.py`.",
        "",
        "## Whole archive (all products — kept untouched in the raw zone)",
        f"- Manifest record count: **{manifest['record_count']:,}** · Spark row count: **{total:,}** "
        f"({'match' if total == manifest['record_count'] else 'MISMATCH — investigate'})",
        f"- Archive SHA-256: `{manifest['archive_sha256']}`",
        f"- Date received range: {overall['min']} → {overall['max']} · unparseable dates: {overall['bad_dates']}",
        f"- Header columns ({len(manifest['header'])}): {', '.join(manifest['header'])}",
        f"- Unexpected columns (not in field reference): {unexpected or 'none'}",
        "",
        f"## Option {settings.OPTION_NUMBER} scope — {settings.OPTION_NAME}",
        f"- Records in scope: **{scope_n:,}** ({100 * scope_n / max(total, 1):.3f}% of the archive)",
        f"- Duplicate Complaint IDs in scope: {dup_ids}",
        f"- Missing state: {states_missing}",
        "",
        "### Records by historical product label",
        md_table([(r["label_version"], r["n"], r["from"], r["to"]) for r in by_version],
                 ["Label version", "Records", "First date", "Last date"]),
        "",
        "### Records by year",
        md_table([(r["year"], r["count"]) for r in by_year], ["Year", "Records"]),
        "",
        "### Records by quarter",
        md_table([(f"{r['y']}-Q{r['q']}", r["count"]) for r in by_quarter], ["Quarter", "Records"]),
        "",
        "### Issues by label version",
        md_table([(r["label_version"], r["Issue"], r["count"]) for r in issues], ["Version", "Issue", "Records"]),
        "",
        "### Field completeness in scope (% non-null)",
        md_table(sorted(completeness.items()), ["Field", "% filled"]),
        "",
        "### Timely response",
        md_table([(r["Timely response?"], r["count"]) for r in timely], ["Value", "Records"]),
        "",
        "### Submission channel",
        md_table([(r["Submitted via"], r["count"]) for r in channel], ["Channel", "Records"]),
        "",
    ]
    os.makedirs("docs", exist_ok=True)
    out = f"docs/profiling_{args.ingest_date}.md"
    with open(out, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    print("\n".join(lines))
    print(f"\nwritten {out}")


if __name__ == "__main__":
    main()
