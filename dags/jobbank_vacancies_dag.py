"""
DAG: jobbank_vacancies
======================
Monthly ingestion of job vacancy data from Job Bank Canada (open.canada.ca)
into the jobbank_vacancies PostgreSQL table.

Schedule:
  Runs on the 1st of every month at 08:00 UTC.
  Each run processes data for the PREVIOUS month:
    - Run on Feb 1  → loads jan2026
    - Run on Mar 1  → loads feb2026
    - etc.

  This is standard Airflow behaviour: data_interval_start always points
  to the start of the previous interval, not the current one.

Backfill:
  start_date=2026-01-01 + catchup=True means Airflow will automatically
  create and execute all missed runs starting from January 2026
  when the DAG is first enabled.

Data source:
  https://open.canada.ca/data/en/dataset/ea639e28-c0fc-48bf-b5dd-b8899bd43072
  Files are UTF-16LE tab-separated CSVs, ~30-40 MB each.
  The resource URL changes every month, so the DAG queries the CKAN API
  first to get the current download link.

Task pipeline:
  get_source_month → get_csv_url → process_and_load

  Strings (source_month, url) travel between tasks via XCom.
  download + clean + upsert are merged into one task because each Airflow
  task runs in an isolated process — /tmp is not shared between tasks.

DB connection:
  PG_JOBBANK_CONN is stored as an Airflow Variable containing a PostgreSQL
  connection URI (postgresql://user:pass@host:port/db).
  It is read via Variable.get() and passed directly to psycopg2.
"""

from __future__ import annotations

import io
import logging
from datetime import datetime, date, timedelta

import psycopg2
import requests
import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable

log = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────

# CKAN API — returns the list of all dataset resources (80+ monthly files)
CKAN_PACKAGE_URL = (
    "https://open.canada.ca/data/api/3/action/package_show"
    "?id=ea639e28-c0fc-48bf-b5dd-b8899bd43072"
)

# PG_JOBBANK_CONN is an Airflow Variable containing a PostgreSQL URI
PG_CONN_VAR  = "PG_JOBBANK_CONN"
TARGET_TABLE = "jobbank_vacancies"

# Mapping of CSV column names → target table columns.
# We select only the 14 fields we need out of 65 columns in the source file.
COLUMN_MAP = {
    "WIC Job Location Snapshot ID": "wic_job_location_snapshot_id",
    "Job Title":                    "job_title",
    "NOC21 Code":                   "noc21_code",
    "NOC21 Code Name":              "noc21_code_name",
    "First Posting Date":           "first_posting_date",
    "Vacancy Count":                "vacancy_count",
    "Province/Territory":           "province_territory",
    "City":                         "city",
    "Employment Type":              "employment_type",
    "Employment Term":              "employment_term",
    "Employment Term Telework":     "employment_term_telework",
    "Salary Per":                   "salary_per",
    "Salary Minimum":               "salary_minimum",
    "Salary Maximum":               "salary_maximum",
}

# ─── Tasks ────────────────────────────────────────────────────────────────────

@task
def get_source_month(data_interval_start=None) -> str:
    """
    Derive the target month string from the interval start date.
    For a run on Feb 1, data_interval_start is Jan 1 → returns 'jan2026'.
    """
    execution_date: date = data_interval_start.date()
    source_month = execution_date.strftime("%b%Y").lower()
    log.info("source_month=%s", source_month)
    return source_month


@task
def get_csv_url(source_month: str) -> str:
    """
    Query the CKAN API and return the download URL for the given month.

    The resource URL changes every month (different resource_id), so we
    look it up dynamically instead of hardcoding it.
    """
    resp = requests.get(CKAN_PACKAGE_URL, timeout=30)
    resp.raise_for_status()
    resources = resp.json()["result"]["resources"]

    # File names always follow the same pattern, only the month suffix changes
    filename = f"job-bank-open-data-all-job-postings-en-{source_month}.csv"
    for resource in resources:
        url: str = resource.get("url", "")
        if url.endswith(filename):
            log.info("Found URL: %s", url)
            return url

    raise ValueError(
        f"No resource found for month '{source_month}'. "
        f"The file may not be published yet (usually available ~7 days after month end). "
        f"Available files: {[r.get('url', '').split('/')[-1] for r in resources]}"
    )


@task
def process_and_load(url: str, source_month: str) -> int:
    """
    Download the CSV, clean it, and upsert into PostgreSQL — all in one task.

    Keeping these three steps in a single task avoids the /tmp sharing problem:
    each Airflow task runs in an isolated process, so files written to /tmp
    by one task are not visible to the next.

    Steps:
    1. Download — UTF-16LE tab-separated CSV, ~30-40 MB
    2. Clean    — filter columns, cast types, replace "NA" with NULL
    3. Upsert   — ON CONFLICT DO UPDATE, batches of 5000 rows
    """
    # ── 1. Download ──────────────────────────────────────────────────────────
    log.info("Downloading: %s", url)
    resp = requests.get(url, timeout=300, stream=True)
    resp.raise_for_status()

    df = pd.read_csv(
        io.BytesIO(resp.content),
        sep="\t",
        encoding="utf-16-le",
        dtype=str,       # keep everything as str to preserve NOC code leading zeros
        low_memory=False,
    )
    log.info("Downloaded %d rows, %d columns", len(df), len(df.columns))

    # ── 2. Clean ─────────────────────────────────────────────────────────────
    available = [c for c in COLUMN_MAP if c in df.columns]
    df = df[available].rename(columns=COLUMN_MAP).copy()

    # Add the month label — used as part of the UNIQUE constraint
    df["source_month"] = source_month

    # "NA" in the source means a missing value; replace with None
    df = df.where(df != "NA", other=None)
    df = df.where(df.notna(), other=None)

    if "first_posting_date" in df.columns:
        df["first_posting_date"] = pd.to_datetime(
            df["first_posting_date"], format="%Y/%m/%d", errors="coerce"
        ).dt.date

    for col in ("salary_minimum", "salary_maximum"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            # NUMERIC(10,2) max is 99_999_999.99 — null out garbage values
            df.loc[df[col].abs() >= 1e8, col] = None

    if "vacancy_count" in df.columns:
        df["vacancy_count"] = pd.to_numeric(df["vacancy_count"], errors="coerce")

    if "wic_job_location_snapshot_id" in df.columns:
        df["wic_job_location_snapshot_id"] = pd.to_numeric(
            df["wic_job_location_snapshot_id"], errors="coerce"
        )

    log.info("Cleaned data: %d rows ready for upsert", len(df))

    # ── 3. Upsert ─────────────────────────────────────────────────────────────
    columns = [
        "wic_job_location_snapshot_id", "job_title", "noc21_code", "noc21_code_name",
        "first_posting_date", "vacancy_count", "province_territory", "city",
        "employment_type", "employment_term", "employment_term_telework",
        "salary_per", "salary_minimum", "salary_maximum", "source_month",
    ]

    # Conflict is on UNIQUE(wic_job_location_snapshot_id, source_month) — update the rest
    conflict_keys = {"wic_job_location_snapshot_id", "source_month"}
    update_set = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in columns if c not in conflict_keys
    )

    sql = f"""
        INSERT INTO {TARGET_TABLE} ({', '.join(columns)})
        VALUES ({', '.join(['%s'] * len(columns))})
        ON CONFLICT (wic_job_location_snapshot_id, source_month)
        DO UPDATE SET {update_set}
    """

    # pd.isna() doesn't handle None inside tuples, so we cast NaN → None explicitly
    rows = [
        tuple(None if pd.isna(v) else v for v in row)
        for row in df[columns].itertuples(index=False, name=None)
    ]

    # Read the connection URI from Airflow Variables and connect via psycopg2
    conn_uri = Variable.get(PG_CONN_VAR)
    conn = psycopg2.connect(conn_uri)
    cur  = conn.cursor()

    BATCH = 5000
    for i in range(0, len(rows), BATCH):
        cur.executemany(sql, rows[i : i + BATCH])
        log.info("Upserted rows %d–%d", i, min(i + BATCH, len(rows)))

    conn.commit()
    cur.close()
    conn.close()

    log.info("Done — %d rows loaded", len(rows))
    return len(rows)


# ─── DAG definition ───────────────────────────────────────────────────────────

@dag(
    dag_id="jobbank_vacancies",
    description="Monthly ingestion of Job Bank Canada vacancies into PostgreSQL",
    start_date=datetime(2026, 1, 1),     # backfill starts from January 2026
    schedule_interval="0 8 1 * *",       # 1st of every month at 08:00 UTC
    catchup=True,                        # auto-creates past runs for backfill
    max_active_runs=3,                   # up to 3 months running in parallel during backfill
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "owner": "data-team",
    },
    tags=["jobbank", "canada", "vacancies"],
)
def jobbank_vacancies():
    source_month = get_source_month()
    url          = get_csv_url(source_month)
    process_and_load(url, source_month)


jobbank_vacancies()
