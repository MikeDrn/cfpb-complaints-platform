"""Load curated Parquet into MongoDB (idempotent upserts by Complaint ID) and refresh the aggregates.

python -m database.load_mongo --run-id full_2026-11-20
"""
import argparse
import logging
from datetime import datetime, timezone

from pymongo import MongoClient, ReplaceOne
from pymongo.errors import CollectionInvalid

from config import settings
from database import indexes
from database.schema import AGGREGATES, COMPLAINT_VALIDATOR, COMPLAINTS, META, to_datetime, to_doc
from ingestion.storage import get_json
from processing.spark_session import get_spark

log = logging.getLogger("load_mongo")
AGG_PATH = f"s3a://{settings.BUCKET_CURATED}/aggregates/option={settings.OPTION_NUMBER}"
BATCH = 1000


def ensure_collection(db):
    try:
        db.create_collection(COMPLAINTS, validator=COMPLAINT_VALIDATOR, validationAction="error")
    except CollectionInvalid:
        db.command("collMod", COMPLAINTS, validator=COMPLAINT_VALIDATOR, validationAction="error")


def write_partition(rows):
    """Runs inside Spark workers: bulk upserts, 1,000 documents per round trip."""
    from pymongo import MongoClient as _Client
    client = _Client(settings.MONGO_URI)
    coll = client[settings.MONGO_DB][COMPLAINTS]
    loaded_at = datetime.now(timezone.utc)
    batch = []
    for row in rows:
        doc = to_doc(row.asDict(), loaded_at)
        batch.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))   # same ID -> same document
        if len(batch) == BATCH:
            coll.bulk_write(batch, ordered=False)
            batch = []
    if batch:
        coll.bulk_write(batch, ordered=False)
    client.close()


def replace_collection(db, name, docs, index_keys):
    """Build into a temporary collection, then rename: the dashboard never reads a half-built aggregate."""
    tmp = db[f"{name}_tmp"]
    tmp.drop()
    if docs:
        tmp.insert_many(docs)
    else:
        db.create_collection(f"{name}_tmp")
    for keys in index_keys:
        tmp.create_index(keys)
    tmp.rename(name, dropTarget=True)


AGG_INDEXES = {
    "agg_quarterly": [[("year_quarter", 1)], [("issue_group", 1), ("year_quarter", 1)]],
    "agg_state_issue": [[("state", 1)]],
    "agg_response": [[("year", 1), ("issue_group", 1)]],
    "agg_channel": [[("year", 1)]],
}


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    spark = get_spark("cfpb-load-mongo")
    db = MongoClient(settings.MONGO_URI)[settings.MONGO_DB]
    ensure_collection(db)
    indexes.create(db)

    curated = spark.read.parquet(settings.CURATED_PATH).persist()
    curated_ids = curated.count()

    # 1. complaint documents: upsert every curated record (safe to repeat)
    curated.repartition(4).foreachPartition(write_partition)

    # 2. documents no longer in curated (should not happen; reported, never silently kept)
    ids = {r["complaint_id"] for r in curated.select("complaint_id").collect()}
    stale = [d["_id"] for d in db[COMPLAINTS].find({}, {"_id": 1}) if d["_id"] not in ids]
    if stale:
        db[COMPLAINTS].delete_many({"_id": {"$in": stale}})
        log.warning("removed %s documents that are no longer in the curated layer", len(stale))

    # 3. aggregates: rebuilt every time the curated records change
    for name in AGGREGATES:
        rows = [r.asDict() for r in spark.read.parquet(f"{AGG_PATH}/{name}").collect()]
        replace_collection(db, name, rows, AGG_INDEXES[name])
        log.info("aggregate %s: %s rows", name, len(rows))

    # 4. reconciliation + freshness
    mongo_docs = db[COMPLAINTS].count_documents({})
    latest = curated.agg({"date_received": "max"}).first()[0]
    quality = get_json(settings.BUCKET_QUALITY, f"run_id={args.run_id}/quality_report.json") or {}
    db[META].replace_one({"_id": "freshness"}, {
        "_id": "freshness",
        "option": settings.OPTION_NUMBER, "option_name": settings.OPTION_NAME,
        "latest_date_received": to_datetime(latest),
        "latest_successful_ingestion": quality.get("freshness", {}).get("latest_successful_ingestion"),
        "serving_refreshed_at": datetime.now(timezone.utc),
        "last_run_id": args.run_id,
        "curated_records": curated_ids, "mongo_documents": mongo_docs,
        "reconciled": curated_ids == mongo_docs,
    }, upsert=True)
    if quality:
        db[META].replace_one({"_id": "quality_latest"}, {"_id": "quality_latest", **quality}, upsert=True)

    print(f"curated records: {curated_ids} | MongoDB documents: {mongo_docs} | "
          f"{'RECONCILED' if curated_ids == mongo_docs else 'MISMATCH'}")
    if curated_ids != mongo_docs:
        raise SystemExit("Reconciliation failed between curated layer and MongoDB")


if __name__ == "__main__":
    main()
