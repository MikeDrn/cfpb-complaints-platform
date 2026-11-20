"""MongoDB document model for option 11 (one document per complaint) and collection validators."""
from datetime import date, datetime, time, timezone

from bson.int64 import Int64

COMPLAINTS = "complaints"
AGGREGATES = ["agg_quarterly", "agg_state_issue", "agg_response", "agg_channel"]
META = "meta"

# $jsonSchema validator: MongoDB itself rejects documents with wrong types (brief 6.4: document schema and types)
COMPLAINT_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["_id", "complaint_id", "date_received", "year", "quarter", "year_quarter",
                     "product", "issue", "company", "lineage"],
        "properties": {
            "_id": {"bsonType": "long", "description": "Complaint ID (stable business key, unique)"},
            "complaint_id": {"bsonType": "long"},
            "date_received": {"bsonType": "date", "description": "native BSON date (UTC midnight)"},
            "date_sent_to_company": {"bsonType": ["date", "null"]},
            "year": {"bsonType": "int"},
            "quarter": {"bsonType": "int", "minimum": 1, "maximum": 4},
            "year_quarter": {"bsonType": "string", "pattern": "^[0-9]{4}-Q[1-4]$"},
            "product": {"bsonType": "object", "required": ["name", "label_version"]},
            "issue": {"bsonType": "object", "required": ["name", "group"]},
            "company": {"bsonType": "object", "required": ["name"],
                        "properties": {"timely_response": {"bsonType": ["bool", "null"]}}},
            "tags": {"bsonType": "array"},
            "quality_flags": {"bsonType": "array"},
        },
    }
}


def to_datetime(d):
    """BSON has no date-only type: store dates as UTC midnight datetimes so range filters and sorting work."""
    if d is None:
        return None
    if isinstance(d, datetime):
        return d
    if isinstance(d, date):
        return datetime.combine(d, time.min, tzinfo=timezone.utc)
    raise TypeError(f"unexpected date value {d!r}")


def to_doc(r: dict, loaded_at=None) -> dict:
    """Curated Parquet row -> MongoDB document. Original source labels are kept next to harmonised ones."""
    timely = r.get("timely_response")
    cid = Int64(int(r["complaint_id"]))       # int64: Python ints below 2^31 would otherwise be stored as int32
    return {
        "_id": cid,
        "complaint_id": cid,
        "date_received": to_datetime(r["date_received"]),
        "date_sent_to_company": to_datetime(r.get("date_sent_to_company")),
        "year": int(r["year"]), "quarter": int(r["quarter"]), "month": int(r["month"]),
        "year_quarter": r["year_quarter"], "year_month": r["year_month"],
        "product": {"name": r["product"], "sub_product": r.get("sub_product"),
                    "label_version": r["label_version"]},
        "issue": {"name": r["issue"], "sub_issue": r.get("sub_issue"), "group": r["issue_group"]},
        "company": {"name": r.get("company"),
                    "response_to_consumer": r.get("company_response_to_consumer"),
                    "public_response": r.get("company_public_response"),
                    "timely_response": None if timely is None else timely == "Yes"},
        "location": {"state": r.get("state"), "zip_code": r.get("zip_code")},
        "submitted_via": r.get("submitted_via"),
        "tags": list(r.get("tags_array") or []),
        "is_servicemember": bool(r.get("is_servicemember")),
        "is_older_american": bool(r.get("is_older_american")),
        "processing_lag_days": r.get("processing_lag_days"),
        "lag_status": r.get("lag_status"),
        "quality_flags": list(r.get("quality_flags") or []),
        "lineage": {"source": r.get("_src"), "run_id": r.get("_run_id"),
                    "ingest_date": r.get("_ingest_date"), "raw_object": r.get("_raw_object")},
        "loaded_at": loaded_at or datetime.now(timezone.utc),
    }
