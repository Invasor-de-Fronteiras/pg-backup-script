#!/usr/bin/env python3
"""
PostgreSQL Backup → S3

Required env vars: PG_HOST, PG_DB, PG_USER, PG_PASSWORD, S3_BUCKET
Optional env vars: PG_PORT, PG_VERSION, S3_REGION, S3_PREFIX, DUMP_PATH,
                   EXCLUDE_TABLES, INCLUDE_TABLES
"""

import gzip
import logging
import os
import shutil
import subprocess
import sys

from datetime import datetime, timezone
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def fatal(msg):
    log.critical(msg)
    sys.exit(1)

# -------------------------------------------------------
# Config — environment variables
# -------------------------------------------------------
def require(key):
    val = os.environ.get(key)
    if not val:
        fatal(f"Environment variable {key} is required but not set.")

    return val

PG_HOST     = require("PG_HOST")
PG_DB       = require("PG_DB")
PG_USER     = require("PG_USER")
PG_PASSWORD = require("PG_PASSWORD")
S3_BUCKET   = require("S3_BUCKET")

PG_PORT        = os.environ.get("PG_PORT",        "5432")
PG_VERSION     = os.environ.get("PG_VERSION",     "16")
S3_REGION      = os.environ.get("S3_REGION",      "us-east-1")
S3_PREFIX      = os.environ.get("S3_PREFIX",      "").strip("/")
DUMP_PATH      = os.environ.get("DUMP_PATH",      "/tmp")
EXCLUDE_TABLES = os.environ.get("EXCLUDE_TABLES", "")  # comma-separated, e.g. "logs,sessions"
INCLUDE_TABLES = os.environ.get("INCLUDE_TABLES", "")  # comma-separated, e.g. "users,orders"


# -------------------------------------------------------
# Functions
# -------------------------------------------------------
def find_pgdump():
    found = shutil.which("pg_dump")
    if found:
        log.info(f"pg_dump found on PATH: {found}")
        return found

    fatal(
        f"pg_dump not found for PostgreSQL {PG_VERSION}. "
        f"Install with: apt install postgresql-client-{PG_VERSION}"
    )


def dump(pg_dump_bin):
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    out_path = Path(DUMP_PATH) / f"{PG_DB}_{timestamp}.dump.gz"

    cmd = [pg_dump_bin, "-h", PG_HOST, "-p", PG_PORT, "-U", PG_USER, "-Fc", "-d", PG_DB]

    if INCLUDE_TABLES and EXCLUDE_TABLES:
        fatal("INCLUDE_TABLES and EXCLUDE_TABLES cannot be set at the same time.")

    if INCLUDE_TABLES:
        for table in INCLUDE_TABLES.split(","):
            cmd += ["-t", table.strip()]
        log.info(f"Including tables: {INCLUDE_TABLES}")

    if EXCLUDE_TABLES:
        for table in EXCLUDE_TABLES.split(","):
            cmd += ["-T", table.strip()]
        log.info(f"Excluding tables: {EXCLUDE_TABLES}")

    log.info(f"Starting dump: {PG_DB}@{PG_HOST}:{PG_PORT}")
    log.info(f"Compressing and writing dump to: {out_path}")
    log.info(f"Running command: {' '.join(cmd)}")

    # Stream pg_dump stdout directly into gzip — avoids loading dump into memory
    pg_dump_proc = subprocess.Popen(
        cmd,
        env={**os.environ, "PGPASSWORD": PG_PASSWORD},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    with gzip.open(out_path, "wb", compresslevel=6) as f:
        shutil.copyfileobj(pg_dump_proc.stdout, f)
    pg_dump_proc.wait()
    proc = pg_dump_proc

    if proc.returncode != 0:
        out_path.unlink(missing_ok=True)
        fatal(f"pg_dump failed:\n{proc.stderr.decode()}")

    if out_path.stat().st_size == 0:
        out_path.unlink(missing_ok=True)
        fatal("Dump file is empty, aborting upload.")

    log.info(f"Dump complete: {out_path.name} ({out_path.stat().st_size / 1_048_576:.2f} MB)")
    return out_path


def upload(file_path):
    key = f"{S3_PREFIX}/{file_path.name}" if S3_PREFIX else file_path.name
    log.info(f"Uploading to s3://{S3_BUCKET}/{key}")

    boto3.client("s3", region_name=S3_REGION).upload_file(
        str(file_path),
        S3_BUCKET,
        key,
        ExtraArgs={"StorageClass": "STANDARD"},
        Config=TransferConfig(multipart_threshold=50 * 1024 * 1024),  # multipart above 50MB
    )
    log.info(f"Upload complete: s3://{S3_BUCKET}/{key}")


def main():
    dump_path = dump(find_pgdump())
    try:
        log.info(f"Dump created at: {dump_path}")
        upload(dump_path)
    finally:
        log.info(f"Cleaning up local dump file: {dump_path}")
        dump_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
