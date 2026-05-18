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

DB connection:
  Airflow Connection ID: PG_JOBBANK_CONN (type: postgres)
"""

from __future__ import annotations

import io
import logging
import os
from datetime import datetime, date, timedelta

import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────

# CKAN API — returns the list of all dataset resources (80+ monthly files)
CKAN_PACKAGE_URL = (
    "https://open.canada.ca/data/api/3/action/package_show"
    "?id=ea639e28-c0fc-48bf-b5dd-b8899bd43072"
)

PG_CONN_ID   = "PG_JOBBANK_CONN"
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

# DDL file is located next to this file in the sql/ folder
SQL_CREATE = os.path.join(
    os.path.dirname(__file__), "..", "sql", "create_jobbank_vacancies.sql"
)

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _source_month(execution_date: date) -> str:
    """
    Convert the interval start date to a month string like 'jan2026', 'feb2026', etc.
    This string is used as the source_month key in the table and as the
    filename suffix on open.canada.ca.
    """
    return execution_date.strftime("%b%Y").lower()


def _get_csv_url(source_month: str) -> str:
    """
    Query the CKAN API and return the download URL of the English CSV for the given month.

    The resource URL changes every month (different resource_id), so we can't
    hardcode it — we need to look it up via the API each time.
    """
    resp = requests.get(CKAN_PACKAGE_URL, timeout=30)
    resp.raise_for_status()
    resources = resp.json()["result"]["resources"]

    # File names always follow the same pattern, only the month suffix changes
    filename = f"job-bank-open-data-all-job-postings-en-{source_month}.csv"
    for resource in resources:
        url: str = resource.get("url", "")
        if url.endswith(filename):
            log.info("Found resource URL for %s: %s", source_month, url)
            return url

    raise ValueError(
        f"No resource found for month '{source_month}'. "
        f"The file may not be published yet (usually available ~7 days after month end). "
        f"Available files: {[r.get('url','').split('/')[-1] for r in resources]}"
    )


def _download_csv(url: str) -> pd.DataFrame:
    """
    Download the CSV file and return a DataFrame.

    File format quirks:
    - Encoding: UTF-16LE (despite the .csv extension)
    - Delimiter: tab
    - Size: ~30-40 MB
    - All values read as strings (dtype=str) to preserve leading zeros in NOC codes
    """
    log.info("Downloading: %s", url)
    resp = requests.get(url, timeout=300, stream=True)
    resp.raise_for_status()

    df = pd.read_csv(
        io.BytesIO(resp.content),
        sep="\t",
        encoding="utf-16-le",
        dtype=str,
        low_memory=False,
    )
    log.info("Downloaded %d rows, %d columns", len(df), len(df.columns))
    return df


def _clean(df: pd.DataFrame, source_month: str) -> pd.DataFrame:
    """
    Keep only the required columns, rename them, and cast to the correct types.

    Cleaning rules:
    - String "NA" → NULL  (used in the source file for missing values)
    - Dates in "YYYY/MM/DD" format → Python date
    - salary_minimum / salary_maximum / vacancy_count / wic_id → numeric
    """
    # Only keep columns that are both in COLUMN_MAP and present in the file
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

    if "vacancy_count" in df.columns:
        df["vacancy_count"] = pd.to_numeric(df["vacancy_count"], errors="coerce")

    if "wic_job_location_snapshot_id" in df.columns:
        df["wic_job_location_snapshot_id"] = pd.to_numeric(
            df["wic_job_location_snapshot_id"], errors="coerce"
        )

    return df


def _upsert(df: pd.DataFrame) -> None:
    """
    Batch-upsert the DataFrame into the target table.

    Uses ON CONFLICT DO UPDATE so re-running the DAG for the same month
    updates existing rows instead of creating duplicates.
    Rows are inserted in batches of 5000 to keep memory usage predictable.
    """
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = hook.get_conn()
    cur  = conn.cursor()

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

    BATCH = 5000
    for i in range(0, len(rows), BATCH):
        cur.executemany(sql, rows[i : i + BATCH])
        log.info("Upserted rows %d–%d", i, min(i + BATCH, len(rows)))

    conn.commit()
    cur.close()
    conn.close()
    log.info("Done — %d rows loaded", len(rows))


# ─── Task functions ───────────────────────────────────────────────────────────

def create_table(**context) -> None:
    """
    Create the table and indexes if they don't exist yet (idempotent).
    DDL is read from sql/create_jobbank_vacancies.sql.
    """
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    with open(SQL_CREATE) as f:
        ddl = f.read()
    hook.run(ddl)
    log.info("Table %s is ready", TARGET_TABLE)


def fetch_and_load(**context) -> None:
    """
    Main task: download the CSV for the target month and upsert into the DB.

    data_interval_start is the beginning of the period we're collecting data for.
    For a run on Feb 1 this is Jan 1, so source_month becomes 'jan2026'.
    """
    execution_date: date = context["data_interval_start"].date()
    source_month = _source_month(execution_date)
    log.info("Processing source_month=%s", source_month)

    url = _get_csv_url(source_month)
    df  = _download_csv(url)
    df  = _clean(df, source_month)
    _upsert(df)


# ─── DAG definition ───────────────────────────────────────────────────────────

with DAG(
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
) as dag:

    t_create = PythonOperator(
        task_id="create_table",
        python_callable=create_table,
        doc_md="Creates the `jobbank_vacancies` table and indexes (IF NOT EXISTS).",
    )

    t_load = PythonOperator(
        task_id="fetch_and_load",
        python_callable=fetch_and_load,
        doc_md="Downloads the monthly CSV from open.canada.ca and upserts into PostgreSQL.",
    )

    # create_table always runs first to guarantee the table exists before loading
    t_create >> t_load
