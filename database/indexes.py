"""Indexes designed around the dashboard filters (date range + issue group, state, company, channel, tags).

python -m database.indexes          create them
python -m database.indexes --drop   drop secondary indexes (the unique _id index always stays)
"""
import argparse

from pymongo import ASCENDING, MongoClient

from config import settings
from database.schema import COMPLAINTS

# Equality field first, then the date range (MongoDB ESR rule: Equality, Sort, Range)
COMPLAINT_INDEXES = [
    ([("date_received", ASCENDING)], "date"),
    ([("issue.group", ASCENDING), ("date_received", ASCENDING)], "issue_group_date"),
    ([("location.state", ASCENDING), ("date_received", ASCENDING)], "state_date"),
    ([("company.name", ASCENDING), ("date_received", ASCENDING)], "company_date"),
    ([("submitted_via", ASCENDING), ("date_received", ASCENDING)], "channel_date"),
    ([("product.label_version", ASCENDING), ("date_received", ASCENDING)], "label_date"),
    ([("tags", ASCENDING), ("date_received", ASCENDING)], "tags_date"),          # multikey (array field)
]


def create(db):
    for keys, name in COMPLAINT_INDEXES:
        db[COMPLAINTS].create_index(keys, name=name)
    return [i["name"] for i in db[COMPLAINTS].list_indexes()]


def drop(db):
    db[COMPLAINTS].drop_indexes()          # never drops the unique _id index


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--drop", action="store_true")
    args = parser.parse_args()
    db = MongoClient(settings.MONGO_URI)[settings.MONGO_DB]
    if args.drop:
        drop(db)
        print("secondary indexes dropped; remaining:", [i["name"] for i in db[COMPLAINTS].list_indexes()])
    else:
        print("indexes:", create(db))


if __name__ == "__main__":
    main()
