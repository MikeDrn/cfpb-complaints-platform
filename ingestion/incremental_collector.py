"""Scheduled incremental collection from the CFPB complaint API (brief 6.2).

* Window   : [high-water mark - LOOKBACK_DAYS, today], requested one calendar day at a time
             (date_received_min = date_received_max = day; the end date is inclusive, verified 17 Nov 2026).
* Filter   : the option's product / sub-product values from config/scope_option<N>.json.
* Paging   : search_after cursor (<epoch_ms>_<complaint_id> of the last hit); frm/page are ignored by the API.
* Failures : bounded retries on timeouts, connection errors, HTTP 429 and 5xx; other 4xx stop the run.
* Raw first: every response page is stored in MinIO BEFORE any processing, including empty pages.
* Run log  : ops/runs/run_id=<id>/run.json (window, parameters, pagination state, retries, objects, counts, status).
* High-water mark: proposed by the run, committed by the pipeline only after processing and loading succeed.
"""
import argparse
import logging
import time
import uuid
from datetime import date, datetime, timedelta, timezone

import requests

from config import settings
from ingestion.storage import get_json, put_json, s3_client

log = logging.getLogger("incremental")
RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class CollectorError(RuntimeError):
    pass


def utcnow():
    return datetime.now(timezone.utc)


def iso(ts):
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def build_params(day, cursor, size, products):
    params = {
        "date_received_min": day.isoformat(),
        "date_received_max": day.isoformat(),
        "size": size,
        "sort": "created_date_asc",
        "no_aggs": "true",
        "product": list(products),          # requests sends product=...&product=...
    }
    if cursor:
        params["search_after"] = cursor
    return params


def fetch_page(session, url, params, max_retries=4, timeout=30, sleep=time.sleep):
    """Returns (payload, {"http_status", "retries"}). Never retries forever."""
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, params=params, timeout=timeout)
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
        else:
            if resp.status_code == 200:
                try:
                    return resp.json(), {"http_status": 200, "retries": attempt - 1}
                except ValueError:
                    last_error = "HTTP 200 with invalid JSON body"
            else:
                last_error = f"HTTP {resp.status_code}"
                if resp.status_code not in RETRYABLE_STATUS:
                    raise CollectorError(f"Non-retryable {last_error}")
        if attempt < max_retries:
            wait = min(60, 2 ** attempt)
            log.warning("attempt %s/%s failed (%s); retrying in %ss", attempt, max_retries, last_error, wait)
            sleep(wait)
    raise CollectorError(f"Gave up after {max_retries} attempts: {last_error}")


def extract_hits(payload):
    hits = payload.get("hits", {})
    total = hits.get("total", 0)
    if isinstance(total, dict):
        total = total.get("value", 0)
    return [h.get("_source", {}) for h in hits.get("hits", [])], int(total or 0)


def next_cursor(payload):
    hits = payload.get("hits", {}).get("hits", [])
    if not hits or not hits[-1].get("sort"):
        return None
    epoch_ms, cid = hits[-1]["sort"][:2]
    return f"{epoch_ms}_{cid}"


def collect_day(session, url, day, products, page_size, write_page, sleep=time.sleep, pause=0.0,
                max_pages=200, max_retries=4, timeout=30):
    cursor, page, records, pages = None, 1, 0, []
    max_date, max_id, truncated, total = None, None, False, 0
    while True:
        payload, meta = fetch_page(session, url, build_params(day, cursor, page_size, products),
                                   max_retries=max_retries, timeout=timeout, sleep=sleep)
        docs, total = extract_hits(payload)
        obj = write_page(day, page, payload)                 # raw first, always (even when empty)
        pages.append({"page": page, "search_after": cursor, **meta, "returned": len(docs), "object": obj})
        records += len(docs)
        for doc in docs:
            d = str(doc.get("date_received") or "")[:10]
            if d and (max_date is None or d > max_date):
                max_date = d
            try:
                cid = int(doc.get("complaint_id"))
                max_id = cid if max_id is None else max(max_id, cid)
            except (TypeError, ValueError):
                pass
        new_cursor = next_cursor(payload)
        page += 1
        if len(docs) < page_size or records >= total or new_cursor is None:
            break
        if new_cursor == cursor or page > max_pages:          # safety: the loop always ends
            truncated = True
            log.warning("day %s: pagination stopped early at page %s", day, page)
            break
        cursor = new_cursor
        sleep(pause)
    return {"day": day.isoformat(), "status": "ok" if records else "empty", "pages": len(pages),
            "records": records, "total_reported": total, "truncated": truncated,
            "max_date_received": max_date, "max_complaint_id": max_id, "requests": pages}


def default_window(today=None, watermark=None):
    """[high-water mark - overlap, today]. Without a watermark, only the overlap before today."""
    today = today or utcnow().date()
    anchor = date.fromisoformat(watermark["last_date_received"]) if watermark else today
    return anchor - timedelta(days=settings.LOOKBACK_DAYS), today


def collect(start=None, end=None, session=None, s3=None, sleep=time.sleep):
    s3 = s3 or s3_client()
    watermark = get_json(settings.BUCKET_OPS, settings.WATERMARK_KEY, s3=s3)
    window_source = "manual"
    if start is None or end is None:
        start, end = default_window(watermark=watermark)
        window_source = "high_water_mark"
    if start > end:
        raise ValueError("start must be on or before end")
    session = session or requests.Session()
    session.headers.update(settings.HTTP_HEADERS)

    started = utcnow()
    ingest_date = started.date().isoformat()
    run_id = f"api_{started:%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex[:6]}"
    raw_prefix = f"cfpb/api/ingest_date={ingest_date}/run_id={run_id}"
    run_key = f"runs/run_id={run_id}/run.json"

    def write_page(day, page, payload):
        key = f"{raw_prefix}/response_{day.isoformat()}_{page:03d}.json"
        put_json(settings.BUCKET_RAW, key, payload, s3)
        return f"{settings.BUCKET_RAW}/{key}"

    run = {"run_id": run_id, "type": "api_incremental", "option": settings.OPTION_NUMBER,
           "started_at": iso(started), "endpoint": settings.CFPB_API_URL,
           "ingest_date": ingest_date, "raw_prefix": raw_prefix,
           "window": {"start": start.isoformat(), "end": end.isoformat(), "source": window_source,
                      "lookback_days": settings.LOOKBACK_DAYS, "date_field": "date_received",
                      "watermark_at_start": watermark},
           "params": {"product": settings.API_PRODUCT_PARAMS, "size": settings.API_PAGE_SIZE,
                      "sort": "created_date_asc", "pagination": "search_after",
                      "max_retries": settings.API_MAX_RETRIES, "timeout_s": settings.API_TIMEOUT_SECONDS},
           "status": "running"}
    put_json(settings.BUCKET_OPS, run_key, run, s3)
    log.info("run %s: window %s -> %s (%s)", run_id, start, end, window_source)

    days = []
    try:
        day = start
        while day <= end:
            result = collect_day(session, settings.CFPB_API_URL, day, settings.API_PRODUCT_PARAMS,
                                 settings.API_PAGE_SIZE, write_page, sleep=sleep, pause=settings.API_PAUSE_SECONDS,
                                 max_retries=settings.API_MAX_RETRIES, timeout=settings.API_TIMEOUT_SECONDS)
            days.append(result)
            log.info("day %s: %s records (%s)", result["day"], result["records"], result["status"])
            day += timedelta(days=1)
        run["status"] = "collected" if any(d["records"] for d in days) else "collected_empty"
    except Exception as exc:
        run["status"] = "failed"
        run["error"] = f"{type(exc).__name__}: {exc}"
        run["failed_on_day"] = day.isoformat()
        raise
    finally:
        dates = [d["max_date_received"] for d in days if d["max_date_received"]]
        ids = [d["max_complaint_id"] for d in days if d["max_complaint_id"] is not None]
        run.update({
            "ended_at": iso(utcnow()),
            "days": days,
            "record_count": sum(d["records"] for d in days),
            "empty_days": [d["day"] for d in days if d["status"] == "empty"],
            "http_requests": sum(len(d["requests"]) for d in days),
            "retries": sum(r["retries"] for d in days for r in d["requests"]),
            "raw_objects": [r["object"] for d in days for r in d["requests"]],
            "truncated_days": [d["day"] for d in days if d["truncated"]],
            "proposed_watermark": ({"last_date_received": max(dates),
                                    "last_complaint_id": max(ids) if ids else None} if dates else None),
        })
        put_json(settings.BUCKET_OPS, run_key, run, s3)
    return run


def commit_watermark(proposed, run_id, s3=None):
    """Called only after processing and loading succeeded. The mark never moves backwards."""
    current = get_json(settings.BUCKET_OPS, settings.WATERMARK_KEY, s3=s3)
    if not proposed:
        return current
    if current and current["last_date_received"] >= proposed["last_date_received"]:
        return current
    new = {**proposed, "set_by_run": run_id, "updated_at": iso(utcnow())}
    put_json(settings.BUCKET_OPS, settings.WATERMARK_KEY, new, s3)
    return new


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--end", type=date.fromisoformat)
    a = parser.parse_args()
    r = collect(a.start, a.end)
    print(f"{r['run_id']}: {r['record_count']} records, status {r['status']}")
