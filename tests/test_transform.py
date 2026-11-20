"""Spark transformation tests (brief section 10): invalid dates, missing values, option filter,
historical label handling and duplicate Complaint IDs."""
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from config import settings
from processing.schema import REQUIRED_CSV_HEADERS
from processing.transform import (UNMAPPED_ISSUE, build_aggregates, deduplicate, duplicate_stats,
                                  load_issue_mapping, transform)

V1, V2, V3 = (f["product"] for f in settings.SCOPE_FILTERS)
MAPPING = load_issue_mapping()


def row(cid="1", received="2025-03-10", product=V3, sub="Payday loan", issue="Charged fees or interest you didn't expect",
        state="TX", timely="Yes", sent="2025-03-10", tags=None, response="Closed with explanation"):
    values = {"Date received": received, "Product": product, "Sub-product": sub, "Issue": issue, "Sub-issue": None,
              "Company public response": None, "Company": "LENDER A", "State": state, "ZIP code": "75001",
              "Tags": tags, "Submitted via": "Web", "Date sent to company": sent,
              "Company response to consumer": response, "Timely response?": timely, "Complaint ID": cid}
    return tuple(values[h] for h in REQUIRED_CSV_HEADERS)


def make_df(spark, rows, src="full_load", prio=1, ts="2026-11-20 18:11:40"):
    schema = StructType([StructField(h, StringType(), True) for h in REQUIRED_CSV_HEADERS])
    return (spark.createDataFrame(rows, schema)
            .withColumn("_src", F.lit(src)).withColumn("_src_priority", F.lit(prio))
            .withColumn("_ingest_date", F.lit("2026-11-20")).withColumn("_run_id", F.lit("test"))
            .withColumn("_ingest_ts", F.to_timestamp(F.lit(ts))).withColumn("_raw_object", F.lit("test.csv")))


def run(spark, rows, **kw):
    return transform(make_df(spark, rows, **kw), settings.SCOPE_FILTERS, MAPPING)


def test_columns_are_standardised(spark):
    valid, _, _ = run(spark, [row()])
    assert {"complaint_id", "date_received", "timely_response", "year_quarter", "label_version",
            "issue_group"} <= set(valid.columns)


def test_option_filter_keeps_three_payday_labels_and_drops_other_sub_products(spark):
    rows = [row("1", "2015-01-05", V1, None, "Can't contact lender"),
            row("2", "2019-01-05", V2, "Payday loan", "Struggling to pay your loan"),
            row("3", "2025-01-05", V3, "Payday loan"),
            row("4", "2025-01-05", V3, "Title loan"),
            row("5", "2025-01-05", "Credit card", None)]
    valid, rejected, filtered_out = run(spark, rows)
    labels = {r["complaint_id"]: r["label_version"] for r in valid.collect()}
    assert labels == {1: "v1_2013_2017", 2: "v2_2017_2023", 3: "v3_2023_now"}
    assert filtered_out.count() == 2 and rejected.count() == 0


def test_historical_issue_wording_maps_to_same_group_and_keeps_original(spark):
    rows = [row("1", "2015-01-05", V1, None, "Charged fees or interest I didn't expect"),
            row("2", "2025-01-05", V3, "Payday loan", "Charged fees or interest you didn't expect")]
    valid, _, _ = run(spark, rows)
    got = {r["complaint_id"]: (r["issue"], r["issue_group"]) for r in valid.collect()}
    assert got[1] == ("Charged fees or interest I didn't expect", "Unexpected fees or interest")
    assert got[2] == ("Charged fees or interest you didn't expect", "Unexpected fees or interest")


def test_unknown_issue_is_flagged_not_dropped(spark):
    valid, _, _ = run(spark, [row(issue="A brand new CFPB issue")])
    r = valid.first()
    assert r["issue_group"] == UNMAPPED_ISSUE and "unmapped_issue" in r["quality_flags"]


def test_invalid_and_missing_dates_are_rejected_with_reason(spark):
    _, rejected, _ = run(spark, [row("1", "2024-13-45"), row("2", None), row("3", "03/10/2025")])
    reasons = {r["complaint_id_raw"]: r["reject_reason"] for r in rejected.collect()}
    assert reasons == {"1": "invalid_date_received", "2": "missing_date_received"}


def test_missing_and_non_numeric_ids_are_rejected(spark):
    _, rejected, _ = run(spark, [row(cid=None), row(cid="ABC")])
    assert sorted(r["reject_reason"] for r in rejected.collect()) == ["invalid_complaint_id", "missing_complaint_id"]


def test_quarter_and_lag_derivation(spark):
    valid, _, _ = run(spark, [row(received="2025-08-30", sent="2025-09-02")])
    r = valid.first()
    assert (r["year_quarter"], r["processing_lag_days"], r["lag_status"]) == ("2025-Q3", 3, "ok")


def test_negative_and_missing_referral_dates_are_flagged(spark):
    valid, _, _ = run(spark, [row("1", "2025-03-10", sent="2025-03-01"), row("2", sent=None)])
    got = {r["complaint_id"]: (r["lag_status"], r["quality_flags"]) for r in valid.collect()}
    assert got[1] == ("negative", ["negative_processing_lag"])
    assert got[2] == ("missing_date", ["missing_date_sent"])


def test_timely_values_normalised_and_invalid_flagged(spark):
    valid, _, _ = run(spark, [row("1", timely=" yes "), row("2", timely="NO"), row("3", timely="maybe")])
    got = {r["complaint_id"]: (r["timely_response"], r["quality_flags"]) for r in valid.collect()}
    assert got[1][0] == "Yes" and got[2][0] == "No" and got[3] == (None, ["invalid_timely_value"])


def test_empty_strings_become_null_and_missing_state_flagged(spark):
    valid, _, _ = run(spark, [row(state="  ")])
    r = valid.first()
    assert r["state"] is None and "missing_state" in r["quality_flags"]


def test_tags_split_into_flags(spark):
    valid, _, _ = run(spark, [row(tags="Older American, Servicemember")])
    r = valid.first()
    assert r["is_older_american"] and r["is_servicemember"] and r["tags_array"] == ["Older American", "Servicemember"]


def test_dedup_prefers_api_then_latest_and_counts_conflicts(spark):
    full, _, _ = run(spark, [row("7", state="NY")])
    api_old, _, _ = run(spark, [row("7", state="CA")], src="api", prio=2, ts="2026-11-21 02:00:00")
    api_new, _, _ = run(spark, [row("7", state="TX")], src="api", prio=2, ts="2026-11-22 02:00:00")
    both = full.unionByName(api_old).unionByName(api_new)
    result = deduplicate(both).collect()
    assert len(result) == 1 and result[0]["state"] == "TX"
    assert duplicate_stats(both) == {"duplicate_ids": 1, "duplicate_rows": 3, "ids_with_conflicting_values": 1}


def test_identical_replay_is_not_counted_as_conflict(spark):
    a, _, _ = run(spark, [row("8")])
    stats = duplicate_stats(a.unionByName(a))
    assert stats["duplicate_ids"] == 1 and stats["ids_with_conflicting_values"] == 0
    assert deduplicate(a.unionByName(a)).count() == 1


def test_aggregates_timely_uses_valid_yes_no_only(spark):
    valid, _, _ = run(spark, [row("1", timely="Yes"), row("2", timely="No"), row("3", timely="maybe")])
    q = build_aggregates(valid)["agg_quarterly"].first()
    assert (q["complaints"], q["timely_yes"], q["timely_valid"]) == (3, 1, 2)
