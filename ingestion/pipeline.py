"""End-to-end incremental run: collect -> Spark (incremental) -> MongoDB -> commit high-water mark.
The NiFi schedule and the manual demo trigger both call this same function.

python -m ingestion.pipeline                                    scheduled behaviour (window from the high-water mark)
python -m ingestion.pipeline --start 2026-11-10 --end 2026-11-19   bounded replay window (does not move the mark)
"""
import argparse
import logging
import subprocess
import sys
from datetime import date, datetime, timezone

from config import settings
from ingestion import incremental_collector
from ingestion.storage import get_json, put_json

log = logging.getLogger("pipeline")


def _step(*module_args):
    log.info("running: %s", " ".join(module_args))
    subprocess.run([sys.executable, "-m", *module_args], check=True)


def run_incremental_pipeline(start=None, end=None, commit_watermark=None):
    manual = start is not None and end is not None
    commit = (not manual) if commit_watermark is None else commit_watermark
    run = incremental_collector.collect(start, end)
    run_id, status, error = run["run_id"], "failed", None
    try:
        if run["record_count"] > 0:
            _step("processing.run_processing", "--mode", "incremental", "--run-id", run_id)
            _step("database.load_mongo", "--run-id", run_id)
        else:
            log.info("no records in the window: nothing to process (successful empty run)")
        watermark = incremental_collector.commit_watermark(run["proposed_watermark"], run_id) if commit else None
        status = "succeeded" if run["record_count"] else "succeeded_empty"
    except subprocess.CalledProcessError as exc:
        error = f"step failed: {' '.join(exc.cmd[2:])} (exit {exc.returncode})"
        raise
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        key = f"runs/run_id={run_id}/run.json"
        stored = get_json(settings.BUCKET_OPS, key) or run
        stored.update({"pipeline_status": status, "pipeline_error": error,
                       "watermark_committed": commit and status.startswith("succeeded"),
                       "pipeline_finished_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")})
        put_json(settings.BUCKET_OPS, key, stored)
    return {"run_id": run_id, "status": status, "records_collected": run["record_count"],
            "window": run["window"], "empty_days": run["empty_days"], "retries": run["retries"],
            "watermark": watermark}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--end", type=date.fromisoformat)
    a = parser.parse_args()
    print(run_incremental_pipeline(a.start, a.end))
