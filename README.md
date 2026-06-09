# CanadaJobsPipeline

End-to-end analytics project on the Canadian labour market: **Job Bank open data → Apache Airflow DAG → PostgreSQL → interactive Tableau Public dashboard**.

**Repository:** https://github.com/vladsakharov/CanadaJobsPipeline  
**Dashboard:** https://public.tableau.com/app/profile/YOUR_PROFILE/viz/JobsCanada/JobsCanada**

---

## Architecture

```
open.canada.ca  →  Airflow DAG  →  PostgreSQL  →  Tableau Public
(Job Bank CSV)      (CKAN + ETL)    (jobbank_vacancies)   Jobs Canada Dashboard
```

| Layer | Description |
|-------|-------------|
| **Source** | [Job Bank Canada Open Data](https://open.canada.ca/data/en/dataset/ea639e28-c0fc-48bf-b5dd-b8899bd43072) — monthly vacancy snapshots |
| **Orchestration** | Airflow DAG `jobbank_vacancies` — download, clean, upsert |
| **Storage** | PostgreSQL table `jobbank_vacancies` |
| **Analytics** | Tableau — KPIs, trends, map, Job Bank drill-down |

---

## Dashboard: Jobs Canada (Jan–May 2026)

Interactive dashboard built on pipeline data — **423K+ vacancies** across Canada.

![Jobs Canada Dashboard](docs/dashboard_screenshot.png)

### Dashboard layout

| Section | What it shows |
|---------|---------------|
| **KPI cards + sparklines** | Total vacancies, latest month + MoM, median pay, on-site % |
| **Monthly Trend** | Vacancies by month + MoM % line (dual axis) |
| **Top 20 NOC** | Most in-demand occupations (NOC 2021) |
| **Map + Web** | Vacancies by province, click → Wikipedia |
| **Detail Table** | Province, city, NOC, salary, pagination, link to Job Bank |
| **Filters** | Province, NOC, Posting Month (applied to all worksheets) |

### Tableau techniques used

- Calculated fields — salary normalization, smart K/M labels, URL fields
- LOD expressions (`FIXED`) — filter-responsive latest-month KPI
- Table calculations — MoM (`LOOKUP`), pagination (`INDEX`)
- Parameters — `Page Number`
- Dual axis — bars + MoM line on Monthly Trend
- Dashboard URL actions — map → Wiki, table → Job Bank, reset → Canada

### Connect data in Tableau

```sql
SELECT * FROM jobbank_vacancies ORDER BY source_month, province_territory;
```

Export to CSV → **Tableau → Connect → Text file**.

---

## Project structure

```
CanadaJobsPipeline/
├── dags/
│   └── jobbank_vacancies_dag.py
├── sql/
│   └── create_jobbank_vacancies.sql
├── docs/
│   └── dashboard-preview.png
├── requirements.txt
└── README.md
```

---

## Data source

**Job Bank Canada — Open Data**  
https://open.canada.ca/data/en/dataset/ea639e28-c0fc-48bf-b5dd-b8899bd43072

- Monthly snapshots of all vacancies posted on jobbank.gc.ca
- Format: UTF-16LE, tab-separated CSV (~30–40 MB per month)
- File URL changes every month → DAG resolves the current link via CKAN API
- New month data is usually published ~7 days after month end

---

## DAG: `jobbank_vacancies`

### Schedule

| Setting | Value |
|---------|-------|
| Cron | `0 8 1 * *` |
| Runs | 1st of every month at 08:00 UTC |
| Processes | **Previous** month (run on Feb 1 → loads `jan2026`) |
| `start_date` | 2026-01-01 |
| `catchup` | `True` |
| `max_active_runs` | 3 |

> Airflow `data_interval_start` points to the start of the completed interval, so the DAG always loads the previous month — not the current one.

### Task pipeline

```
get_source_month → get_csv_url → process_and_load
```

| Task | Description |
|------|-------------|
| `get_source_month` | Derives target month from `data_interval_start` → `jan2026` |
| `get_csv_url` | CKAN API → current CSV download URL |
| `process_and_load` | Download + clean + upsert to PostgreSQL (batches of 5,000) |

Strings (`source_month`, `url`) pass between tasks via XCom.  
Download, clean, and upsert are merged into one task because Airflow tasks run in isolated processes — `/tmp` is not shared across tasks.

### Upsert logic

Conflict key: `UNIQUE(wic_job_location_snapshot_id, source_month)` → `ON CONFLICT DO UPDATE`.  
Re-running the same month updates rows without duplicates.

---

## Database

Table `jobbank_vacancies` — DDL in `sql/create_jobbank_vacancies.sql`.

| Column | Type | Description |
|--------|------|-------------|
| `wic_job_location_snapshot_id` | BIGINT | Job Bank snapshot ID |
| `job_title` | VARCHAR(255) | Job title |
| `noc21_code` | VARCHAR(10) | NOC 2021 code |
| `noc21_code_name` | VARCHAR(255) | Occupation name |
| `first_posting_date` | DATE | First posting date |
| `vacancy_count` | SMALLINT | Number of vacancies |
| `province_territory` | VARCHAR(100) | Province / territory |
| `city` | VARCHAR(100) | City |
| `employment_type` | VARCHAR(50) | Full time / Part time |
| `employment_term` | VARCHAR(100) | Permanent / Temporary |
| `employment_term_telework` | VARCHAR(10) | Yes / No |
| `salary_per` | VARCHAR(20) | Hour / Week / Month / Year |
| `salary_minimum` | NUMERIC(10,2) | Min salary |
| `salary_maximum` | NUMERIC(10,2) | Max salary |
| `source_month` | VARCHAR(20) | jan2026, feb2026, … |
| `created_at` | TIMESTAMP | Load timestamp |

Source CSV has 65 columns — 14 are loaded into the database.

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Airflow Variable

Create **`PG_JOBBANK_CONN`** with a PostgreSQL URI:

```bash
airflow variables set PG_JOBBANK_CONN "postgresql://user:pass@host:5432/db"
```

Or via Airflow UI: **Admin → Variables → +**.

### 3. Create the table

```bash
psql -h <host> -U <user> -d <db> -f sql/create_jobbank_vacancies.sql
```

### 4. Backfill

```bash
airflow dags backfill jobbank_vacancies \
  --start-date 2026-01-01 \
  --end-date   2026-05-01
```

---

## Notes

- If a monthly file is not published yet, the task fails and retries 2× (every 10 minutes).
- `city` and salary fields are often `NULL` — expected for this dataset.

---

## Author

**Vladislav Sakharov**

---

## License

MIT — see [LICENSE](LICENSE)
