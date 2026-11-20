"""Collector tests (brief section 10): known date window, pagination, timeout or HTTP failure,
empty response, repeated request, and high-water-mark behaviour."""
from datetime import date

import pytest
import requests
import responses

from config import settings
from ingestion import incremental_collector as ic

URL = "https://example.test/api/v1/"
NO_SLEEP = lambda seconds: None


def page(ids, total):
    return {"hits": {"total": {"value": total, "relation": "eq"},
                     "hits": [{"_id": str(i), "sort": [1793534400000 + i, str(i)],
                               "_source": {"complaint_id": str(i), "date_received": "2026-11-01T12:00:00.000Z"}}
                              for i in ids]}}


def collect(size=2, **kw):
    pages = []
    out = ic.collect_day(requests.Session(), URL, date(2026, 11, 1), ["Payday loan"], size,
                         lambda d, p, pl: pages.append((p, pl)) or f"raw/{p}", sleep=NO_SLEEP, **kw)
    return out, pages


def test_known_window_requests_one_inclusive_day_with_option_filter():
    params = ic.build_params(date(2026, 11, 1), None, 1000, settings.API_PRODUCT_PARAMS)
    assert params["date_received_min"] == params["date_received_max"] == "2026-11-01"
    assert params["product"] == settings.API_PRODUCT_PARAMS and "search_after" not in params


@responses.activate
def test_pagination_follows_search_after_cursor_and_stores_every_page():
    responses.add(responses.GET, URL, json=page([1, 2], 5))
    responses.add(responses.GET, URL, json=page([3, 4], 5))
    responses.add(responses.GET, URL, json=page([5], 5))
    out, pages = collect()
    assert out["records"] == 5 and [p for p, _ in pages] == [1, 2, 3] and out["max_complaint_id"] == 5
    sent = [c.request.url for c in responses.calls]
    assert "search_after" not in sent[0]
    assert "search_after=1793534400002_2" in sent[1] and "search_after=1793534400004_4" in sent[2]


@responses.activate
def test_successful_empty_response_is_stored_and_marked_empty():
    responses.add(responses.GET, URL, json=page([], 0))
    out, pages = collect(size=1000)
    assert out["status"] == "empty" and out["records"] == 0 and len(pages) == 1


@responses.activate
def test_temporary_server_error_is_retried_then_succeeds():
    responses.add(responses.GET, URL, status=503)
    responses.add(responses.GET, URL, json=page([1], 1))
    _, meta = ic.fetch_page(requests.Session(), URL, {}, sleep=NO_SLEEP)
    assert meta["retries"] == 1


@responses.activate
def test_timeouts_stop_after_the_maximum_number_of_retries():
    for _ in range(4):
        responses.add(responses.GET, URL, body=requests.exceptions.ReadTimeout("slow"))
    with pytest.raises(ic.CollectorError, match="Gave up after 4"):
        ic.fetch_page(requests.Session(), URL, {}, max_retries=4, sleep=NO_SLEEP)
    assert len(responses.calls) == 4


@responses.activate
def test_client_error_is_not_retried():
    responses.add(responses.GET, URL, status=400)
    with pytest.raises(ic.CollectorError, match="Non-retryable HTTP 400"):
        ic.fetch_page(requests.Session(), URL, {}, sleep=NO_SLEEP)
    assert len(responses.calls) == 1


@responses.activate
def test_repeated_request_returns_the_same_records():
    responses.add(responses.GET, URL, json=page([1, 2], 2))
    responses.add(responses.GET, URL, json=page([1, 2], 2))
    first, _ = collect(size=1000)
    second, _ = collect(size=1000)
    assert first["records"] == second["records"] == 2 and first["max_complaint_id"] == second["max_complaint_id"]


@responses.activate
def test_cursor_that_does_not_move_cannot_loop_forever():
    for _ in range(3):
        responses.add(responses.GET, URL, json=page([1, 2], 10))
    out, _ = collect(size=2)
    assert out["truncated"] is True and len(responses.calls) == 2


def test_window_overlaps_the_high_water_mark():
    start, end = ic.default_window(today=date(2026, 11, 21), watermark={"last_date_received": "2026-11-18"})
    assert start == date(2026, 10, 28) and end == date(2026, 11, 21)        # 21-day overlap


def test_high_water_mark_never_moves_backwards(monkeypatch):
    store = {"wm": {"last_date_received": "2026-11-18"}}
    monkeypatch.setattr(ic, "get_json", lambda *a, **k: store["wm"])
    monkeypatch.setattr(ic, "put_json", lambda b, k, o, s3=None: store.update(wm=o))
    assert ic.commit_watermark({"last_date_received": "2026-11-10"}, "old")["last_date_received"] == "2026-11-18"
    assert ic.commit_watermark({"last_date_received": "2026-11-20"}, "new")["last_date_received"] == "2026-11-20"
