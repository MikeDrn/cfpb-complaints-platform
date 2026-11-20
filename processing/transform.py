"""Validation, cleaning, standardisation, option filter, label harmonisation, derivation, deduplication.

All functions are pure DataFrame -> DataFrame so they can be unit-tested without MinIO.
"""
import csv
import os
from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

from processing.schema import CANONICAL_COLUMNS, REQUIRED_SOURCE_COLUMNS, VALID_STATES, canonical_name

LINEAGE_PREFIX = "_"          # _src, _src_priority, _ingest_date, _run_id, _ingest_ts, _raw_object
NULL_TOKENS = ["", "N/A", "n/a", "null", "NULL", "None"]
UNMAPPED_ISSUE = "Other / unmapped"
FLAG_NAMES = [
    "missing_date_sent", "invalid_date_sent", "negative_processing_lag",
    "invalid_timely_value", "missing_state", "invalid_state",
    "future_date_received", "unmapped_issue",
]
BUSINESS_COLUMNS = [c for c in CANONICAL_COLUMNS if c != "complaint_id"]
ISSUE_MAPPING_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                  "config", "issue_mapping.csv")


# ---------------------------------------------------------------- 1. columns and strings
def standardize_columns(df: DataFrame) -> DataFrame:
    """CSV headers and API field names -> one canonical snake_case schema, all strings."""
    for col in df.columns:
        if not col.startswith(LINEAGE_PREFIX) and canonical_name(col) != col:
            df = df.withColumnRenamed(col, canonical_name(col))
    missing = [c for c in REQUIRED_SOURCE_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Source is missing required columns: {missing}")
    for col in CANONICAL_COLUMNS:
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None).cast("string"))
    lineage = [c for c in df.columns if c.startswith(LINEAGE_PREFIX)]
    df = df.select(*CANONICAL_COLUMNS, *lineage)
    for field in df.schema.fields:
        if field.name.startswith(LINEAGE_PREFIX):
            continue
        if isinstance(field.dataType, ArrayType):
            df = df.withColumn(field.name, F.concat_ws(", ", F.col(field.name)))
        elif not isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, F.col(field.name).cast("string"))
    return df


def clean_strings(df: DataFrame) -> DataFrame:
    """Trim every text field; empty strings and null tokens become real nulls."""
    for col in CANONICAL_COLUMNS:
        trimmed = F.trim(F.col(col))
        df = df.withColumn(col, F.when(trimmed.isin(*NULL_TOKENS), F.lit(None)).otherwise(trimmed))
    return df


# ---------------------------------------------------------------- 2. option scope
def _filter_condition(f):
    cond = F.col("product") == f["product"]
    if f.get("sub_product"):
        cond = cond & (F.col("sub_product") == f["sub_product"])
    return cond


def apply_scope(df: DataFrame, scope_filters):
    """Returns (in_scope_with_label_version, filtered_out). Original labels are kept."""
    label = None
    for f in scope_filters:
        c = _filter_condition(f)
        label = F.when(c, f["label_version"]) if label is None else label.when(c, f["label_version"])
    tagged = df.withColumn("label_version", label)
    return tagged.filter(F.col("label_version").isNotNull()), tagged.filter(F.col("label_version").isNull())


# ---------------------------------------------------------------- 3. issue harmonisation
def load_issue_mapping(path=ISSUE_MAPPING_FILE):
    with open(path, newline="", encoding="utf-8") as fh:
        return {row["source_issue"].strip(): row["issue_group"].strip() for row in csv.DictReader(fh)}


def add_issue_group(df: DataFrame, mapping) -> DataFrame:
    if not mapping:
        return df.withColumn("issue_group", F.lit(UNMAPPED_ISSUE))
    expr = reduce(lambda acc, kv: acc.when(F.col("issue") == kv[0], kv[1]),
                  list(mapping.items())[1:],
                  F.when(F.col("issue") == list(mapping.items())[0][0], list(mapping.items())[0][1]))
    return df.withColumn("issue_group", F.coalesce(expr, F.lit(UNMAPPED_ISSUE)))


# ---------------------------------------------------------------- 4. types, derived fields, flags
def _parse_date(col_name: str):
    col = F.col(col_name)
    return F.coalesce(
        F.to_date(F.substring(col, 1, 10), "yyyy-MM-dd"),   # CSV and API ISO formats
        F.to_date(col, "MM/dd/yyyy"),
        F.to_date(col, "MM/dd/yy"),
    )


def derive_fields(df: DataFrame) -> DataFrame:
    timely = F.lower(F.col("timely_response"))
    df = (
        df.withColumn("_tmp_cid_raw", F.col("complaint_id"))
        .withColumn("_tmp_date_received_raw", F.col("date_received"))
        .withColumn("_tmp_date_sent_raw", F.col("date_sent_to_company"))
        .withColumn("_tmp_timely_raw", F.col("timely_response"))
        .withColumn("complaint_id", F.when(F.col("complaint_id").rlike(r"^\d+$"),
                                           F.col("complaint_id").cast("long")))
        .withColumn("date_received", _parse_date("date_received"))
        .withColumn("date_sent_to_company", _parse_date("date_sent_to_company"))
        .withColumn("timely_response",
                    F.when(timely.isin("yes", "y", "true"), "Yes").when(timely.isin("no", "n", "false"), "No"))
        .withColumn("state", F.upper(F.col("state")))
        .withColumn("year", F.year("date_received"))
        .withColumn("month", F.month("date_received"))
        .withColumn("quarter", F.quarter("date_received"))
        .withColumn("year_month", F.date_format("date_received", "yyyy-MM"))
        .withColumn("year_quarter", F.concat(F.col("year").cast("string"), F.lit("-Q"), F.col("quarter").cast("string")))
        # CFPB receipt -> referral to company. NOT the company's resolution time.
        .withColumn("processing_lag_days", F.datediff("date_sent_to_company", "date_received"))
        .withColumn("lag_status",
                    F.when(F.col("_tmp_date_sent_raw").isNull(), "missing_date")
                     .when(F.col("date_sent_to_company").isNull(), "invalid_date")
                     .when(F.col("processing_lag_days") < 0, "negative")
                     .otherwise("ok"))
        .withColumn("tags_array",
                    F.when(F.col("tags").isNull(), F.array().cast("array<string>"))
                     .otherwise(F.split(F.col("tags"), r"\s*,\s*")))
        .withColumn("is_older_american", F.array_contains("tags_array", "Older American"))
        .withColumn("is_servicemember", F.array_contains("tags_array", "Servicemember"))
    )
    flags = F.array(
        F.when(F.col("lag_status") == "missing_date", F.lit("missing_date_sent")),
        F.when(F.col("lag_status") == "invalid_date", F.lit("invalid_date_sent")),
        F.when(F.col("lag_status") == "negative", F.lit("negative_processing_lag")),
        F.when(F.col("_tmp_timely_raw").isNotNull() & F.col("timely_response").isNull(), F.lit("invalid_timely_value")),
        F.when(F.col("state").isNull(), F.lit("missing_state")),
        F.when(F.col("state").isNotNull() & ~F.col("state").isin(*VALID_STATES), F.lit("invalid_state")),
        F.when(F.col("date_received") > F.current_date(), F.lit("future_date_received")),
        F.when(F.col("issue_group") == UNMAPPED_ISSUE, F.lit("unmapped_issue")),
    )
    df = df.withColumn("quality_flags", F.filter(flags, lambda x: x.isNotNull()))
    reason = (
        F.when(F.col("_tmp_cid_raw").isNull(), F.lit("missing_complaint_id"))
        .when(F.col("complaint_id").isNull(), F.lit("invalid_complaint_id"))
        .when(F.col("_tmp_date_received_raw").isNull(), F.lit("missing_date_received"))
        .when(F.col("date_received").isNull(), F.lit("invalid_date_received"))
    )
    return df.withColumn("_tmp_reject_reason", reason)


# ---------------------------------------------------------------- 5. full transformation
def transform(df: DataFrame, scope_filters, issue_mapping):
    """Returns (valid, rejected, filtered_out). Rejected rows are quarantined with a reason, never dropped silently."""
    df = clean_strings(standardize_columns(df))
    in_scope, filtered_out = apply_scope(df, scope_filters)
    # In-scope rows are few (about 19,000), so they are cached; the 19-million-row raw input is never cached.
    out = derive_fields(add_issue_group(in_scope, issue_mapping)).persist()
    tmp_cols = [c for c in out.columns if c.startswith("_tmp_")]
    valid = out.filter(F.col("_tmp_reject_reason").isNull()).drop(*tmp_cols)
    rejected = (out.filter(F.col("_tmp_reject_reason").isNotNull())
                .withColumn("reject_reason", F.col("_tmp_reject_reason"))
                .withColumn("complaint_id_raw", F.col("_tmp_cid_raw"))
                .withColumn("date_received_raw", F.col("_tmp_date_received_raw"))
                .drop(*tmp_cols))
    return valid, rejected, filtered_out


# ---------------------------------------------------------------- 6. deduplication
def deduplicate(df: DataFrame) -> DataFrame:
    """One row per complaint_id.
    Rule: prefer the API version (_src_priority 2) over the full load (1), because it is the more
    recent view of the complaint; between versions of the same source, keep the latest _ingest_ts;
    final tie-breaker: highest date_sent_to_company, so the result is deterministic."""
    window = Window.partitionBy("complaint_id").orderBy(
        F.col("_src_priority").desc(), F.col("_ingest_ts").desc_nulls_last(),
        F.col("date_sent_to_company").desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")


def duplicate_stats(df: DataFrame) -> dict:
    """How many IDs appear more than once, and how many of those have conflicting business values."""
    row_hash = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in BUSINESS_COLUMNS]), 256)
    groups = (df.withColumn("_h", row_hash).groupBy("complaint_id")
              .agg(F.count("*").alias("n"), F.countDistinct("_h").alias("versions"))
              .filter("n > 1"))
    row = groups.agg(F.count("*").alias("ids"), F.sum("n").alias("rows"),
                     F.sum(F.when(F.col("versions") > 1, 1).otherwise(0)).alias("conflicting")).first()
    return {"duplicate_ids": int(row["ids"] or 0), "duplicate_rows": int(row["rows"] or 0),
            "ids_with_conflicting_values": int(row["conflicting"] or 0)}


# ---------------------------------------------------------------- 7. dashboard-ready aggregates
def build_aggregates(curated: DataFrame) -> dict:
    timely_yes = F.sum(F.when(F.col("timely_response") == "Yes", 1).otherwise(0)).alias("timely_yes")
    timely_valid = F.sum(F.when(F.col("timely_response").isin("Yes", "No"), 1).otherwise(0)).alias("timely_valid")
    lag_ok = F.col("lag_status") == "ok"
    return {
        "agg_quarterly": curated.groupBy("year", "quarter", "year_quarter", "label_version", "issue_group").agg(
            F.count("*").alias("complaints"), timely_yes, timely_valid,
            F.sum(F.when(lag_ok, F.col("processing_lag_days"))).alias("lag_days_sum"),
            F.sum(F.when(lag_ok, 1).otherwise(0)).alias("lag_valid"),
            F.sum(F.col("is_servicemember").cast("int")).alias("servicemember"),
            F.sum(F.col("is_older_american").cast("int")).alias("older_american")),
        "agg_state_issue": curated.groupBy("state", "issue_group").agg(F.count("*").alias("complaints")),
        "agg_response": curated.groupBy("year", "issue_group", "company_response_to_consumer").agg(
            F.count("*").alias("complaints"), timely_yes, timely_valid),
        "agg_channel": curated.groupBy("year", "submitted_via").agg(F.count("*").alias("complaints")),
    }
