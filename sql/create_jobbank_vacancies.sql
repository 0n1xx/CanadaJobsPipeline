CREATE TABLE IF NOT EXISTS jobbank_vacancies (
    id                              SERIAL PRIMARY KEY,

    wic_job_location_snapshot_id    BIGINT,
    job_title                       VARCHAR(255),
    noc21_code                      VARCHAR(10),
    noc21_code_name                 VARCHAR(255),
    first_posting_date              DATE,
    vacancy_count                   SMALLINT,
    province_territory              VARCHAR(100),
    city                            VARCHAR(100),
    employment_type                 VARCHAR(50),    -- Full time / Part time
    employment_term                 VARCHAR(100),   -- Permanent / Temporary
    employment_term_telework        VARCHAR(10),    -- Yes / No
    salary_per                      VARCHAR(20),    -- Hour / Week / Month / Year
    salary_minimum                  NUMERIC(10, 2),
    salary_maximum                  NUMERIC(10, 2),
    source_month                    VARCHAR(20),    -- feb2026, mar2026 etc.

    created_at                      TIMESTAMP DEFAULT NOW(),

    UNIQUE (wic_job_location_snapshot_id, source_month)
);

CREATE INDEX IF NOT EXISTS idx_province        ON jobbank_vacancies(province_territory);
CREATE INDEX IF NOT EXISTS idx_city            ON jobbank_vacancies(city);
CREATE INDEX IF NOT EXISTS idx_posting_date    ON jobbank_vacancies(first_posting_date);
CREATE INDEX IF NOT EXISTS idx_noc21           ON jobbank_vacancies(noc21_code);
CREATE INDEX IF NOT EXISTS idx_employment_type ON jobbank_vacancies(employment_type);
CREATE INDEX IF NOT EXISTS idx_salary_per      ON jobbank_vacancies(salary_per);
CREATE INDEX IF NOT EXISTS idx_source_month    ON jobbank_vacancies(source_month);
