"""HTTP trigger used by NiFi. One run at a time.
The incremental endpoint is added in the incremental-ingestion part."""
import threading

from fastapi import FastAPI, HTTPException

from ingestion import historical_collector

app = FastAPI(title="CFPB collector")
_lock = threading.Lock()


@app.get("/health")
def health():
    return {"status": "ok"}


def _exclusive(fn, *args):
    if not _lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="A run is already in progress")
    try:
        return fn(*args)
    except HTTPException:
        raise
    except (Exception, SystemExit) as exc:          # SystemExit: our CLIs use it for "nothing to do" errors
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        _lock.release()


@app.post("/run/historical-finalize")
def historical_finalize(ingest_date: str):
    manifest = _exclusive(historical_collector.finalize_from_minio, ingest_date)
    return {"record_count": manifest["record_count"], "archive_sha256": manifest["archive_sha256"]}
