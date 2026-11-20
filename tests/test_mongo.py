"""MongoDB tests (brief section 10): safe upserts, unique index behaviour, correct aggregates, query execution.
Unit tests run anywhere; tests marked 'integration' need the mongo container:
    docker compose run --rm jobs pytest -q -m integration
"""
from datetime import date, datetime

import pytest
from bson.int64 import Int64
from pymongo import MongoClient, ReplaceOne
from pymongo.errors import DuplicateKeyError, WriteError

from config import settings
from database import indexes
from database.queries import QUERIES, measure
from database.schema import COMPLAINT_VALIDATOR, COMPLAINTS, to_doc


def curated_row(cid=30990964, timely="Yes", tags=None, group="Unexpected fees or interest", state="TX"):
    return {"complaint_id": cid, "date_received": date(2025, 3, 10), "date_sent_to_company": date(2025, 3, 11),
            "year": 2025, "quarter": 1, "month": 3, "year_quarter": "2025-Q1", "year_month": "2025-03",
            "product": "Payday loan, title loan, personal loan, or advance loan", "sub_product": "Payday loan",
            "label_version": "v3_2023_now", "issue": "Charged fees or interest you didn't expect", "sub_issue": None,
            "issue_group": group, "company": "LENDER A", "company_response_to_consumer": "Closed with explanation",
            "company_public_response": None, "timely_response": timely, "state": state, "zip_code": "75001",
            "submitted_via": "Web", "tags_array": tags or [], "is_servicemember": "Servicemember" in (tags or []),
            "is_older_american": False, "processing_lag_days": 1, "lag_status": "ok", "quality_flags": [],
            "_src": "full_load", "_run_id": "test", "_ingest_date": "2026-11-20", "_raw_object": "x.csv"}


# ---------------------------------------------------------------- unit tests
def test_document_uses_native_dates_int64_id_and_boolean_timely():
    doc = to_doc(curated_row())
    assert isinstance(doc["_id"], Int64) and doc["_id"] == doc["complaint_id"] == 30990964
    assert isinstance(doc["date_received"], datetime)
    assert doc["company"]["timely_response"] is True
    assert doc["issue"] == {"name": "Charged fees or interest you didn't expect", "sub_issue": None,
                            "group": "Unexpected fees or interest"}


def test_missing_timely_value_stays_null_not_false():
    assert to_doc(curated_row(timely=None))["company"]["timely_response"] is None


# ---------------------------------------------------------------- integration tests (real MongoDB)
@pytest.fixture()
def db():
    client = MongoClient(settings.MONGO_URI, serverSelectionTimeoutMS=3000)
    test_db = client["cfpb_test"]
    test_db.drop_collection(COMPLAINTS)
    test_db.create_collection(COMPLAINTS, validator=COMPLAINT_VALIDATOR, validationAction="error")
    indexes.create(test_db)
    yield test_db
    client.drop_database("cfpb_test")


@pytest.mark.integration
def test_upserting_the_same_records_twice_creates_no_duplicates(db):
    docs = [to_doc(curated_row(cid)) for cid in (1, 2, 3)]
    for _ in range(2):
        db[COMPLAINTS].bulk_write([ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in docs])
    assert db[COMPLAINTS].count_documents({}) == 3


@pytest.mark.integration
def test_unique_id_rejects_a_second_insert(db):
    db[COMPLAINTS].insert_one(to_doc(curated_row(7)))
    with pytest.raises(DuplicateKeyError):
        db[COMPLAINTS].insert_one(to_doc(curated_row(7)))


@pytest.mark.integration
def test_validator_rejects_a_string_date(db):
    bad = to_doc(curated_row(8))
    bad["date_received"] = "2025-03-10"
    with pytest.raises(WriteError):
        db[COMPLAINTS].insert_one(bad)


@pytest.mark.integration
def test_timely_percentage_uses_valid_values_only(db):
    db[COMPLAINTS].insert_many([to_doc(curated_row(1, "Yes")), to_doc(curated_row(2, "No")),
                                to_doc(curated_row(3, None))])
    q3 = list(db[COMPLAINTS].aggregate(QUERIES["Q3 Company response and timely % by issue group"]["pipeline"]))
    assert q3[0]["complaints"] == 3 and q3[0]["timely_pct"] == 50.0


@pytest.mark.integration
def test_state_query_uses_the_index(db):
    db[COMPLAINTS].insert_many([to_doc(curated_row(i, state="OH" if i % 10 == 0 else "TX")) for i in range(1, 201)])
    pipeline = [{"$match": {"location.state": "OH"}}, {"$group": {"_id": "$issue.group", "n": {"$sum": 1}}}]
    after = measure(db, pipeline, repeats=1)
    before = measure(db, pipeline, hint={"$natural": 1}, repeats=1)
    assert "IXSCAN" in after["plan"] and after["docs_examined"] == 20
    assert "COLLSCAN" in before["plan"] and before["docs_examined"] == 200
