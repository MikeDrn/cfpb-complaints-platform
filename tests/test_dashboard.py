"""Dashboard tests (brief section 10): empty selection, invalid date range, missing category,
database unavailable and a basic layout check. Uses an in-memory MongoDB (mongomock)."""
import random
from datetime import date, datetime, timezone

import mongomock
import pytest
import streamlit as st
from streamlit.testing.v1 import AppTest

from dashboard import data as D

GROUPS = D.ISSUE_GROUPS[:6]


def seed(n=400):
    db = mongomock.MongoClient()["cfpb"]
    rnd = random.Random(1)
    docs = []
    for i in range(n):
        y = rnd.choice([2015, 2019, 2024, 2025, 2026])
        q = rnd.randint(1, 3 if y == 2026 else 4)
        d = datetime(y, 3 * q - 1, 10, tzinfo=timezone.utc)
        docs.append({"_id": i, "complaint_id": i, "date_received": d, "year": y, "quarter": q,
                     "year_quarter": f"{y}-Q{q}",
                     "product": {"name": "P", "label_version": "v1_2013_2017" if y < 2017 else
                                 "v2_2017_2023" if y < 2023 else "v3_2023_now"},
                     "issue": {"name": "x", "sub_issue": None, "group": rnd.choice(GROUPS)},
                     "company": {"name": rnd.choice(["LENDER A", "LENDER B"]),
                                 "response_to_consumer": rnd.choice(["Closed with explanation", "Closed with monetary relief"]),
                                 "timely_response": rnd.choice([True, True, False, None])},
                     "location": {"state": rnd.choice(["TX", "CA", "OH", None])},
                     "submitted_via": rnd.choice(["Web", "Phone"]),
                     "is_servicemember": rnd.random() < 0.1, "is_older_american": rnd.random() < 0.06,
                     "processing_lag_days": 2, "lag_status": "ok"})
    db.complaints.insert_many(docs)
    db.meta.insert_one({"_id": "freshness", "latest_date_received": datetime(2026, 11, 18),
                        "serving_refreshed_at": datetime(2026, 11, 20, 20, 0), "reconciled": True,
                        "curated_records": n, "mongo_documents": n})
    db.meta.insert_one({"_id": "quality_latest", "status": "pass", "checks": [{"check": "x", "status": "pass"}],
                        "completeness_pct": {"state": 75.0}})
    return db


# ---------------------------------------------------------------- data layer
def test_invalid_date_range_is_rejected():
    with pytest.raises(D.InvalidFilter):
        D.build_match(date(2026, 1, 1), date(2025, 1, 1))


def test_empty_selection_returns_no_headline():
    db = seed()
    match = D.build_match(date(2016, 1, 1), date(2016, 12, 31))      # no seeded complaints in 2016
    assert D.headline(db, match) is None and D.quarterly(db, match).empty


def test_missing_category_is_labelled_and_selectable():
    db = seed()
    match = D.build_match(date(2014, 1, 1), date(2026, 11, 18))
    states = D.share_by(db, match, "location.state")
    assert D.NOT_PROVIDED in set(states["value"])
    only_missing = D.build_match(date(2014, 1, 1), date(2026, 11, 18), states=[D.NOT_PROVIDED])
    assert D.headline(db, only_missing)["complaints"] == int(states.loc[states["value"] == D.NOT_PROVIDED, "complaints"].iloc[0])


def test_timely_percentage_excludes_missing_values():
    db = seed()
    h = D.headline(db, D.build_match(date(2014, 1, 1), date(2026, 11, 18)))
    assert h["timely_pct"] == pytest.approx(100 * h["timely_yes"] / (h["timely_yes"] + h["timely_no"]))
    assert h["timely_invalid_or_missing"] > 0


def test_shares_add_up_to_100():
    db = seed()
    m = D.mix(db, D.build_match(date(2014, 1, 1), date(2026, 11, 18)), "product.label_version", "issue.group")
    assert m.groupby("outer")["share_pct"].sum().round(6).eq(100).all()


def test_partial_quarters_are_detected():
    partial = D.partial_quarters(["2026-Q3", "2026-Q4", "2014-Q1"], date(2026, 11, 18), date(2014, 2, 1), date(2026, 11, 18))
    assert partial == {"2026-Q4", "2014-Q1"}


def test_unreachable_database_raises_friendly_error():
    with pytest.raises(D.DataUnavailable):
        D.connect("mongodb://127.0.0.1:1/", "cfpb", timeout_ms=200)


# ---------------------------------------------------------------- whole app
def run_app(monkeypatch, db=None, fail=False):
    def fake_connect(*a, **k):
        if fail:
            raise D.DataUnavailable("down")
        return db
    monkeypatch.setattr(D, "connect", fake_connect)
    st.cache_resource.clear()
    st.cache_data.clear()
    at = AppTest.from_file("dashboard/app.py", default_timeout=60)
    at.run()
    return at


def test_layout_renders_all_views_without_errors(monkeypatch):
    at = run_app(monkeypatch, seed())
    assert not at.exception
    assert at.title[0].value == "Payday lending complaints"
    assert len(at.tabs) == 6 and len(at.metric) >= 4
    assert len(at.sidebar.multiselect) >= 3 and len(at.sidebar.date_input) == 1


def test_app_shows_message_when_database_is_down(monkeypatch):
    at = run_app(monkeypatch, fail=True)
    assert not at.exception and "not reachable" in at.error[0].value


def test_app_handles_filters_with_no_results(monkeypatch):
    at = run_app(monkeypatch, seed())
    at.sidebar.date_input[0].set_value((date(2016, 1, 1), date(2016, 12, 31))).run()
    assert not at.exception and "No complaints match" in at.warning[0].value
