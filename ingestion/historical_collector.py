"""Historical (full) load of the official CFPB complaint CSV ZIP into the MinIO raw zone.

The FULL archive is preserved (all products). The option scope is applied later, in Spark
(brief 6.1: keep the raw historical archive separate from the scope used for analysis).

Python path : python -m ingestion.historical_collector [--force]
NiFi path   : NiFi lands the ZIP, then calls the collector service, which runs
              python -m ingestion.historical_collector --finalize-only --ingest-date YYYY-MM-DD
"""
import argparse
import csv
import hashlib
import logging
import os
import shutil
import tempfile
import time
import zipfile
from datetime import datetime, timezone

import requests

from config import settings
from ingestion.storage import exists, put_json, s3_client

log = logging.getLogger("historical")
csv.field_size_limit(2**31 - 1)


def utcnow_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256_of(path):
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download(url, dest, max_retries=3, timeout=60):
    """Stream the file to disk with bounded retries. Never retries forever."""
    for attempt in range(1, max_retries + 1):
        try:
            with requests.get(url, stream=True, timeout=timeout) as resp:
                resp.raise_for_status()
                with open(dest, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        fh.write(chunk)
                return {"http_status": resp.status_code, "attempts": attempt,
                        "http_last_modified": resp.headers.get("Last-Modified"),
                        "http_content_length": resp.headers.get("Content-Length")}
        except requests.RequestException as exc:
            log.warning("download attempt %s/%s failed: %s", attempt, max_retries, exc)
            if attempt == max_retries:
                raise
            time.sleep(10 * attempt)


def extract_and_count(zip_path, workdir):
    with zipfile.ZipFile(zip_path) as zf:
        members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if len(members) != 1:
            raise ValueError(f"Expected exactly one CSV in the archive, found {members}")
        zf.extract(members[0], workdir)
    csv_path = os.path.join(workdir, members[0])
    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)          # csv module handles quoted newlines correctly
        header = next(reader)
        count = sum(1 for _ in reader)
    return csv_path, header, count


def _finalise(zip_path, ingest_date, run_id, source_meta, upload_zip, s3):
    prefix = f"cfpb/full_load/ingest_date={ingest_date}"
    started = utcnow_iso()
    if upload_zip:
        log.info("uploading archive to raw/%s", prefix)
        s3.upload_file(zip_path, settings.BUCKET_RAW, f"{prefix}/complaints.csv.zip")
    workdir = tempfile.mkdtemp(prefix="cfpb_")
    try:
        log.info("extracting and counting records (a few minutes)")
        csv_path, header, count = extract_and_count(zip_path, workdir)
        log.info("uploading extracted CSV (%s records)", count)
        s3.upload_file(csv_path, settings.BUCKET_RAW, f"{prefix}/complaints.csv")
        manifest = {
            "run_id": run_id,
            "source_url": settings.CFPB_CSV_URL,
            "downloaded_at": source_meta.get("downloaded_at", started),
            "landed_by": source_meta.get("landed_by", "python"),
            "archive_object": f"{settings.BUCKET_RAW}/{prefix}/complaints.csv.zip",
            "csv_object": f"{settings.BUCKET_RAW}/{prefix}/complaints.csv",
            "archive_size_bytes": os.path.getsize(zip_path),
            "archive_sha256": sha256_of(zip_path),
            "csv_size_bytes": os.path.getsize(csv_path),
            "csv_sha256": sha256_of(csv_path),
            "header": header,
            "record_count": count,
            "http": {k: v for k, v in source_meta.items() if k.startswith("http") or k == "attempts"},
            "note": "Full archive, all products. Option scope is applied in Spark.",
        }
        put_json(settings.BUCKET_RAW, f"{prefix}/manifest.json", manifest, s3)
        put_json(settings.BUCKET_OPS, f"runs/run_id={run_id}/run.json", {
            "run_id": run_id, "type": "full_load", "started_at": started, "ended_at": utcnow_iso(),
            "source": settings.CFPB_CSV_URL, "raw_prefix": prefix, "ingest_date": ingest_date,
            "record_count": count, "archive_sha256": manifest["archive_sha256"], "outcome": "succeeded",
        }, s3)
        log.info("full load finalised: %s records, sha256=%s", count, manifest["archive_sha256"])
        return manifest
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def run_download(force=False):
    s3 = s3_client()
    ingest_date = datetime.now(timezone.utc).date().isoformat()
    prefix = f"cfpb/full_load/ingest_date={ingest_date}"
    if exists(settings.BUCKET_RAW, f"{prefix}/manifest.json", s3) and not force:
        raise SystemExit(f"Raw data for {ingest_date} already exists. Raw is immutable; use --force only on purpose.")
    run_id = f"full_{ingest_date}"
    tmpdir = tempfile.mkdtemp(prefix="cfpb_dl_")
    try:
        zip_path = os.path.join(tmpdir, "complaints.csv.zip")
        downloaded_at = utcnow_iso()
        log.info("downloading %s", settings.CFPB_CSV_URL)
        meta = download(settings.CFPB_CSV_URL, zip_path)
        meta["downloaded_at"] = downloaded_at
        return _finalise(zip_path, ingest_date, run_id, meta, upload_zip=True, s3=s3)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def finalize_from_minio(ingest_date):
    """NiFi path: the ZIP was landed in MinIO by NiFi; compute checksum, extract, count, write manifest."""
    s3 = s3_client()
    key = f"cfpb/full_load/ingest_date={ingest_date}/complaints.csv.zip"
    if not exists(settings.BUCKET_RAW, key, s3):
        raise SystemExit(f"Archive not found at raw/{key}")
    if exists(settings.BUCKET_RAW, f"cfpb/full_load/ingest_date={ingest_date}/manifest.json", s3):
        raise SystemExit(f"Manifest for {ingest_date} already exists. Raw is immutable.")
    head = s3.head_object(Bucket=settings.BUCKET_RAW, Key=key)
    tmpdir = tempfile.mkdtemp(prefix="cfpb_fin_")
    try:
        zip_path = os.path.join(tmpdir, "complaints.csv.zip")
        s3.download_file(settings.BUCKET_RAW, key, zip_path)
        meta = {"landed_by": "nifi",
                "downloaded_at": head["LastModified"].strftime("%Y-%m-%dT%H:%M:%SZ")}
        return _finalise(zip_path, ingest_date, f"full_{ingest_date}", meta, upload_zip=False, s3=s3)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--finalize-only", action="store_true")
    parser.add_argument("--ingest-date")
    args = parser.parse_args()
    if args.finalize_only:
        finalize_from_minio(args.ingest_date)
    else:
        run_download(force=args.force)
