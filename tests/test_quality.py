from processing.quality import evaluate, load_thresholds


def base_report(**over):
    rep = {
        "reconciliation": {"raw_rows": 100, "expected_raw_rows": 100, "filtered_out": 80, "in_scope": 20,
                           "rejected": 0, "valid_new": 20, "valid": 20, "curated": 20, "duplicates_removed": 0,
                           "check_raw_equals_filtered_plus_in_scope": True,
                           "check_in_scope_equals_valid_plus_rejected": True,
                           "check_valid_equals_curated_plus_duplicates_removed": True},
        "uniqueness": {"duplicates_after": 0},
        "completeness_pct": {c: 100.0 for c in ["complaint_id", "date_received", "product", "issue", "company",
                                                "timely_response", "company_response_to_consumer", "state",
                                                "submitted_via", "date_sent_to_company", "zip_code"]},
        "completeness_where_applicable": {"sub_product": {"pct_filled_where_applicable": 100.0}},
        "validity_flags": {},
        "freshness": {"days_behind_today": 1},
    }
    for k, v in over.items():
        rep[k].update(v) if isinstance(v, dict) else rep.__setitem__(k, v)
    return rep


def status_of(checks, name):
    return next(c["status"] for c in checks if c["check"] == name)


def test_clean_report_passes():
    checks, overall = evaluate(base_report(), load_thresholds())
    assert overall == "pass"


def test_duplicate_ids_in_curated_output_fail_the_run():
    checks, overall = evaluate(base_report(uniqueness={"duplicates_after": 1}), load_thresholds())
    assert status_of(checks, "curated_ids_unique") == "fail" and overall == "fail"


def test_too_many_rejected_records_fail_the_run():
    checks, overall = evaluate(base_report(reconciliation={"rejected": 5}), load_thresholds())
    assert status_of(checks, "rejected_pct") == "fail"


def test_missing_optional_zip_is_only_a_warning():
    checks, overall = evaluate(base_report(completeness_pct={"zip_code": 50.0}), load_thresholds())
    assert status_of(checks, "completeness_zip_code") == "warn" and overall == "warn"


def test_broken_reconciliation_fails():
    checks, overall = evaluate(base_report(reconciliation={"check_raw_equals_filtered_plus_in_scope": False}),
                               load_thresholds())
    assert overall == "fail"
