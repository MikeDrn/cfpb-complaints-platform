"""Spark job: raw zone -> (validate, clean, filter option, harmonise labels, deduplicate) -> quality gate
-> curated Parquet + dashboard-ready aggregates.

Full load  : python -m processing.run_processing --mode full --ingest-date YYYY-MM-DD
Incremental: python -m processing.run_processing --mode incremental --run-id api_...
"""
import argparse
import logging
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from config import settings
from ingestion.storage import get_json, put_json
from processing.quality import QualityGateError, build_report, evaluate, load_thresholds
from processing.schema import REQUIRED_CSV_HEADERS, explicit_string_schema, validate_header
from processing.spark_session import get_spark
from processing.transform import (build_aggregates, deduplicate, duplicate_stats, load_issue_mapping,
                                  transform)

log = logging.getLogger("processing")
AGG_PATH = f"s3a://{settings.BUCKET_CURATED}/aggregates/option={settings.OPTION_NUMBER}"
API_REQUIRED_FIELDS = ["complaint_id", "date_received", "product"]


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def add_lineage(df, src, priority, ingest_date, run_id, ingest_ts):
    return (df.withColumn("_src", F.lit(src))
              .withColumn("_src_priority", F.lit(priority))
              .withColumn("_ingest_date", F.lit(ingest_date))
              .withColumn("_run_id", F.lit(run_id))
              .withColumn("_ingest_ts", F.to_timestamp(F.lit(ingest_ts)))
              .withColumn("_raw_object", F.input_file_name()))


def read_full(spark, ingest_date):
    prefix = f"cfpb/full_load/ingest_date={ingest_date}"
    manifest = get_json(settings.BUCKET_RAW, f"{prefix}/manifest.json")
    if manifest is None:
        raise SystemExit(f"No manifest at raw/{prefix}. Run the historical load first.")
    missing, unexpected = validate_header(manifest["header"])
    if missing:
        raise SystemExit(f"Schema validation failed, missing columns: {missing}")
    if unexpected:
        log.warning("Columns not in the field reference (kept in raw, ignored): %s", unexpected)
    df = (spark.read.schema(explicit_string_schema(manifest["header"]))       # explicit schema, all strings
          .option("header", True).option("multiLine", True).option("escape", '"').option("mode", "PERMISSIVE")
          .csv(f"s3a://{settings.BUCKET_RAW}/{prefix}/complaints.csv")
          .select(*REQUIRED_CSV_HEADERS))
    run_id = f"full_{ingest_date}"
    df = add_lineage(df, "full_load", 1, ingest_date, run_id, manifest["downloaded_at"])
    return df, manifest["record_count"], run_id, manifest["downloaded_at"]


def read_api(spark, run_id):
    run = get_json(settings.BUCKET_OPS, f"runs/run_id={run_id}/run.json")
    if run is None:
        raise SystemExit(f"No run metadata for {run_id}")
    if run.get("record_count", 0) == 0:
        return None, 0, run.get("ended_at")
    payload = (spark.read.option("multiLine", True)
               .json(f"s3a://{settings.BUCKET_RAW}/{run['raw_prefix']}/response_*.json"))
    docs = payload.select(F.explode("hits.hits").alias("h"), F.input_file_name().alias("_f")).select("h._source.*", "_f")
    missing = [c for c in API_REQUIRED_FIELDS if c not in docs.columns]
    if missing:
        raise SystemExit(f"Schema validation failed for API responses, missing fields: {missing}")
    docs = add_lineage(docs, "api", 2, run["ingest_date"], run_id, run["started_at"])
    docs = docs.withColumn("_raw_object", F.col("_f")).drop("_f")
    return docs, run["record_count"], run.get("ended_at")


def read_existing_curated(spark):
    try:
        return spark.read.parquet(settings.CURATED_PATH)
    except AnalysisException:
        return None


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["full", "incremental"], required=True)
    parser.add_argument("--ingest-date")
    parser.add_argument("--run-id")
    args = parser.parse_args()

    spark = get_spark(f"cfpb-option{settings.OPTION_NUMBER}-{args.mode}")
    started = now_iso()
    if args.mode == "full":
        raw_df, expected, run_id, last_ingestion = read_full(spark, args.ingest_date)
    else:
        run_id = args.run_id
        raw_df, expected, last_ingestion = read_api(spark, run_id)
        if raw_df is None:
            put_json(settings.BUCKET_OPS, f"runs/run_id={run_id}/processing.json",
                     {"run_id": run_id, "status": "no_new_records", "finished_at": now_iso()})
            log.info("no records in run %s; curated data unchanged", run_id)
            return

    raw_rows = raw_df.count()                       # pass 1 over the raw input (reconciliation with the manifest)
    valid_new, rejected, _ = transform(raw_df, settings.SCOPE_FILTERS, load_issue_mapping())   # pass 2

    existing = read_existing_curated(spark) if args.mode == "incremental" else None
    previously_curated = existing.count() if existing is not None else 0
    candidates = valid_new if existing is None else existing.unionByName(valid_new, allowMissingColumns=True)
    candidates = candidates.persist()
    dup_stats = duplicate_stats(candidates)
    curated = deduplicate(candidates).localCheckpoint(eager=True)   # materialise before overwriting curated

    counts = {"raw_rows": raw_rows, "expected_raw_rows": expected if args.mode == "full" else None}
    counts["in_scope"] = valid_new.count() + rejected.count()
    counts["filtered_out"] = raw_rows - counts["in_scope"]
    counts["rejected"] = rejected.count()
    counts["valid_new"] = valid_new.count()
    counts["previously_curated"] = previously_curated
    counts["valid"] = candidates.count()
    counts["curated"] = curated.count()
    counts["duplicates_removed"] = counts["valid"] - counts["curated"]

    # ---- quality gate BEFORE publishing
    report = build_report(run_id, args.mode, counts, dup_stats, candidates, curated, rejected, last_ingestion)
    report["checks"], report["status"] = evaluate(report, load_thresholds())
    report["option"] = {"number": settings.OPTION_NUMBER, "name": settings.OPTION_NAME}
    report["generated_at"] = now_iso()
    put_json(settings.BUCKET_QUALITY, f"run_id={run_id}/quality_report.json", report)
    rejected.write.mode("overwrite").json(f"s3a://{settings.BUCKET_QUALITY}/run_id={run_id}/rejected")
    for c in report["checks"]:
        log.info("quality %-48s %-6s value=%s threshold=%s", c["check"], c["status"], c["value"], c["threshold"])
    if report["status"] == "fail":
        put_json(settings.BUCKET_OPS, f"runs/run_id={run_id}/processing.json",
                 {"run_id": run_id, "status": "failed_quality_gate", "finished_at": now_iso()})
        raise QualityGateError(f"Quality gate failed for {run_id}: see quality/run_id={run_id}/quality_report.json")

    # ---- publish curated records and aggregates
    (curated.write.mode("overwrite").option("partitionOverwriteMode", "static")
     .partitionBy("year", "month").parquet(settings.CURATED_PATH))
    for name, agg in build_aggregates(curated).items():
        agg.coalesce(1).write.mode("overwrite").parquet(f"{AGG_PATH}/{name}")

    if args.mode == "full":
        latest = curated.agg(F.max("date_received")).first()[0]
        current = get_json(settings.BUCKET_OPS, settings.WATERMARK_KEY)
        if latest and (current is None or current["last_date_received"] < latest.isoformat()):
            put_json(settings.BUCKET_OPS, settings.WATERMARK_KEY,
                     {"last_date_received": latest.isoformat(), "set_by_run": run_id, "updated_at": now_iso()})

    put_json(settings.BUCKET_OPS, f"runs/run_id={run_id}/processing.json", {
        "run_id": run_id, "mode": args.mode, "status": "succeeded", "quality_status": report["status"],
        "curated_path": settings.CURATED_PATH, "aggregates_path": AGG_PATH,
        "counts": counts, "started_at": started, "finished_at": now_iso(),
    })
    log.info("processing %s finished (%s): %s", run_id, report["status"], counts)


if __name__ == "__main__":
    main()
