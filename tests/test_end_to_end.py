"""End-to-end test (brief section 10): a small sample moves from raw storage through Spark and MongoDB
into the dashboard's data layer, with counts reconciled at every stage.

It never touches the real data: it uses raw/cfpb/e2e/, curated/e2e/ and the MongoDB database cfpb_e2e.
Run it alone (it needs Spark with the S3 connector):
    docker compose run --rm jobs pytest -q -m e2e
"""
import os
from datetime import date

import pytest
from pymongo import MongoClient, ReplaceOne
from pyspark.sql import functions as F

from config import settings
from dashboard import data as D
from database import indexes
from database.schema import COMPLAINT_VALIDATOR, COMPLAINTS, to_doc
from ingestion.storage import s3_client
from processing.schema import REQUIRED_CSV_HEADERS, explicit_string_schema
from processing.spark_session import get_spark
from processing.transform import deduplicate, duplicate_stats, load_issue_mapping, transform

SAMPLE = "tests/data/sample_complaints.csv"
RAW_KEY = "cfpb/e2e/ingest_date=1999-01-01/complaints.csv"
CURATED = f"s3a://{settings.BUCKET_CURATED}/e2e/option={settings.OPTION_NUMBER}"


@pytest.mark.e2e
@pytest.mark.skipif(not os.path.exists(SAMPLE), reason="run processing.make_sample first")
def test_sample_flows_from_raw_to_dashboard_with_reconciled_counts():
    # 1. raw storage: upload the sample exactly as a raw file
    s3 = s3_client()
    s3.upload_file(SAMPLE, settings.BUCKET_RAW, RAW_KEY)

    # 2. Spark: read from MinIO with the explicit schema, transform, deduplicate
    spark = get_spark("cfpb-e2e")
    raw = (spark.read.schema(explicit_string_schema(REQUIRED_CSV_HEADERS)).option("header", True)
           .option("multiLine", True).option("escape", '"').csv(f"s3a://{settings.BUCKET_RAW}/{RAW_KEY}")
           .withColumn("_src", F.lit("full_load")).withColumn("_src_priority", F.lit(1))
           .withColumn("_ingest_date", F.lit("1999-01-01")).withColumn("_run_id", F.lit("e2e"))
           .withColumn("_ingest_ts", F.current_timestamp()).withColumn("_raw_object", F.input_file_name()))
    # add the three known bad records (brief section 10) so the checks are always exercised
    one = raw.filter(F.col("Product") == settings.SCOPE_FILTERS[-1]["product"]) \
             .filter(F.col("Sub-product") == "Payday loan").limit(1)
    bad = (one.withColumn("Company response to consumer", F.lit("Closed with monetary relief"))   # conflicting duplicate
           .unionByName(one.withColumn("Complaint ID", F.lit("999999901")).withColumn("Date received", F.lit("2024-13-45")))
           .unionByName(one.withColumn("Complaint ID", F.lit(None).cast("string"))))
    raw = raw.unionByName(bad).cache()
    raw_n = raw.count()
    valid, rejected, filtered = transform(raw, settings.SCOPE_FILTERS, load_issue_mapping())
    stats = duplicate_stats(valid)
    curated = deduplicate(valid)
    curated.write.mode("overwrite").partitionBy("year", "month").parquet(CURATED)
    curated = spark.read.parquet(CURATED)

    counts = {"raw": raw_n, "filtered_out": filtered.count(), "rejected": rejected.count(),
              "valid": valid.count(), "curated": curated.count()}
    print("e2e counts:", counts, stats)
    assert counts["raw"] == counts["filtered_out"] + counts["rejected"] + counts["valid"]
    assert {"invalid_date_received", "missing_complaint_id"} <= set(r["reject_reason"] for r in rejected.collect())
    assert stats["ids_with_conflicting_values"] >= 1
    assert counts["curated"] == counts["valid"] - (stats["duplicate_rows"] - stats["duplicate_ids"])
    assert curated.select("complaint_id").distinct().count() == counts["curated"]

    # 3. MongoDB: load into an isolated database, twice (safe repeat)
    client = MongoClient(settings.MONGO_URI)
    client.drop_database("cfpb_e2e")
    db = client["cfpb_e2e"]
    db.create_collection(COMPLAINTS, validator=COMPLAINT_VALIDATOR, validationAction="error")
    indexes.create(db)
    docs = [to_doc(r.asDict()) for r in curated.collect()]
    for _ in range(2):
        db[COMPLAINTS].bulk_write([ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in docs])
    assert db[COMPLAINTS].count_documents({}) == counts["curated"]

    # 4. dashboard data layer: the headline figure equals the curated count
    headline = D.headline(db, D.build_match(date(2011, 1, 1), date.today()))
    assert headline["complaints"] == counts["curated"]

    client.drop_database("cfpb_e2e")
    s3.delete_object(Bucket=settings.BUCKET_RAW, Key=RAW_KEY)
