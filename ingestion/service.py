"""HTTP triggers called by NiFi. One run at a time (a second call while running gets HTTP 409)."""
import threading
from datetime import date
from typing import Optional

from fastapi import FastAPI, HTTPException

from config import settings
from ingestion import historical_collector, pipeline
from ingestion.storage import get_json

app = FastAPI(title="CFPB collector")
_lock = threading.Lock()


@app.get("/health")
def health():
    return {"status": "ok", "option": settings.OPTION_NUMBER}


def _exclusive(fn, *args):
    if not _lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="A run is already in progress")
    try:
        return fn(*args)
    except HTTPException:
        raise
    except (Exception, SystemExit) as exc:
        raise HTTPException(status_code=500, detail=f"{type(exc).__name__}: {exc}")
    finally:
        _lock.release()


@app.post("/run/incremental")
def run_incremental(start: Optional[date] = None, end: Optional[date] = None):
    """Scheduled call: no parameters (window from the high-water mark).
    Demo replay: ?start=YYYY-MM-DD&end=YYYY-MM-DD (bounded window, high-water mark unchanged)."""
    return _exclusive(pipeline.run_incremental_pipeline, start, end)


@app.post("/run/historical-finalize")
def historical_finalize(ingest_date: str):
    manifest = _exclusive(historical_collector.finalize_from_minio, ingest_date)
    return {"record_count": manifest["record_count"], "archive_sha256": manifest["archive_sha256"]}


@app.get("/state/watermark")
def watermark():
    return get_json(settings.BUCKET_OPS, settings.WATERMARK_KEY) or {}
