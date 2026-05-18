# CanadaJobsPipeline

ETL-пайплайн для анализа канадского рынка труда на основе открытых данных Job Bank, Apache Airflow, PostgreSQL и Tableau.

---

## Архитектура

```
open.canada.ca  ──►  Airflow DAG  ──►  PostgreSQL  ──►  Tableau
(CSV, monthly)        (parse +          (jobbank_
                       upsert)           vacancies)
```

---

## Источник данных

**Job Bank Canada — Open Data**
[open.canada.ca/data/en/dataset/ea639e28...](https://open.canada.ca/data/en/dataset/ea639e28-c0fc-48bf-b5dd-b8899bd43072)

- Ежемесячные снимки всех вакансий, размещённых на jobbank.gc.ca
- Формат: UTF-16LE, tab-separated CSV (~30-40 MB/месяц)
- Данные появляются примерно через 7 дней после окончания месяца
- URL файла каждый месяц меняется → DAG запрашивает актуальную ссылку через CKAN API

---

## Структура проекта

```
CanadaJobsPipeline/
├── dags/
│   └── jobbank_vacancies_dag.py      # Airflow DAG
├── sql/
│   └── create_jobbank_vacancies.sql  # DDL таблицы и индексов
├── requirements.txt
└── README.md
```

---

## DAG: `jobbank_vacancies`

### Расписание

| Параметр | Значение |
|---|---|
| Cron | `0 8 1 * *` |
| Когда запускается | 1-е число каждого месяца в 08:00 UTC |
| Какой месяц обрабатывает | **Предыдущий** (запуск 1 февраля → данные за январь) |
| Бэкфилл с | 2026-01-01 |
| `catchup` | `True` — Airflow сам создаёт пропущенные запуски |
| `max_active_runs` | 3 — до 3 месяцев параллельно при бэкфилле |

> **Почему предыдущий месяц?**
> В Airflow `data_interval_start` всегда равен началу прошедшего интервала.
> Запуск на 1 февраля имеет `data_interval_start = 1 января` → DAG скачивает `jan2026`.
> Данные за текущий месяц ещё неполные, поэтому такая логика правильна.

### Задачи

```
get_source_month → get_csv_url → download_csv → clean → upsert
```

| Task | Описание |
|---|---|
| `get_source_month` | Определяет целевой месяц из `data_interval_start` → возвращает строку вида `jan2026` |
| `get_csv_url` | Запрашивает CKAN API и возвращает актуальный URL CSV-файла для нужного месяца |
| `download_csv` | Скачивает CSV (~30-40 MB, UTF-16LE), сохраняет как Parquet в `/tmp` |
| `clean` | Фильтрует колонки, приводит типы, заменяет `NA` → `NULL`, сохраняет очищенный Parquet |
| `upsert` | Загружает очищенный Parquet в PostgreSQL батчами по 5000 строк |

Строки (`source_month`, `url`, пути к файлам) передаются между тасками через XCom.
DataFrame-ы слишком велики для XCom, поэтому хранятся во временных Parquet-файлах в `/tmp`
и удаляются после каждого шага.

### Upsert-логика

Конфликт определяется по `UNIQUE(wic_job_location_snapshot_id, source_month)`.
При повторном запуске за тот же месяц данные обновляются, дублей не возникает.

---

## База данных

### Таблица `jobbank_vacancies`

| Колонка | Тип | Описание |
|---|---|---|
| `wic_job_location_snapshot_id` | BIGINT | Уникальный ID снимка из Job Bank |
| `job_title` | VARCHAR(255) | Название вакансии |
| `noc21_code` | VARCHAR(10) | Код NOC 2021 |
| `noc21_code_name` | VARCHAR(255) | Название профессии по NOC 2021 |
| `first_posting_date` | DATE | Дата первой публикации вакансии |
| `vacancy_count` | SMALLINT | Количество вакансий |
| `province_territory` | VARCHAR(100) | Провинция / территория |
| `city` | VARCHAR(100) | Город |
| `employment_type` | VARCHAR(50) | Full time / Part time |
| `employment_term` | VARCHAR(100) | Permanent / Temporary |
| `employment_term_telework` | VARCHAR(10) | Yes / No |
| `salary_per` | VARCHAR(20) | Hour / Week / Month / Year |
| `salary_minimum` | NUMERIC(10,2) | Минимальная зарплата |
| `salary_maximum` | NUMERIC(10,2) | Максимальная зарплата |
| `source_month` | VARCHAR(20) | Метка месяца: jan2026, feb2026, … |
| `created_at` | TIMESTAMP | Время загрузки записи |

---

## Настройка

### 1. Зависимости

```bash
pip install -r requirements.txt
```

`requirements.txt` должен содержать:
```
apache-airflow
apache-airflow-providers-postgres
pandas
requests
```

### 2. Airflow Connection

Создайте подключение с ID **`PG_JOBBANK_CONN`** (тип: `postgres`):

```bash
airflow connections add PG_JOBBANK_CONN \
  --conn-type postgres \
  --conn-host <host> \
  --conn-login <user> \
  --conn-password <password> \
  --conn-schema <database> \
  --conn-port 5432
```

Или через Airflow UI: **Admin → Connections → +**.

### 3. Бэкфилл истории

При первом включении DAG (`catchup=True`) Airflow автоматически создаст все
запуски начиная с `start_date=2026-01-01`. Их выполнение можно ускорить,
увеличив `max_active_runs` на время бэкфилла.

Ручной запуск конкретного диапазона:
```bash
airflow dags backfill jobbank_vacancies \
  --start-date 2026-01-01 \
  --end-date   2026-04-01
```

---

## Примечания

- Данные за текущий месяц появляются на open.canada.ca примерно **через 7 дней после его окончания**.
  Если DAG запустится до публикации файла — задача упадёт с ошибкой и повторит попытку (retry × 2, каждые 10 мин).
- Поле `city` и зарплатные поля часто содержат `NULL` — это норма для данного датасета.
- Исходный CSV содержит 65 колонок; в базу загружаются только 14 нужных.
