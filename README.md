# Job Intelligence Pipeline 🎯

An end-to-end Data Engineering pipeline that scrapes real job listings daily, transforms them through a medallion architecture, and serves insights via an interactive dashboard.

## Architecture
JSearch API → Apache Airflow → PostgreSQL → dbt → Streamlit
## Tech Stack

- **Orchestration:** Apache Airflow 2.8.0
- **Database:** PostgreSQL 15
- **Transformation:** dbt (Bronze → Silver → Gold)
- **API:** JSearch (LinkedIn, Indeed, Glassdoor)
- **Dashboard:** Streamlit
- **Infrastructure:** Docker + Docker Compose

## Features

- Daily job scraping across 3 DE-focused keywords
- Medallion architecture (raw → staging → mart)
- ATS scoring and job relevance ranking
- Ghost job detection
- Application tracker with status pipeline
- 6 automated data quality tests

## Setup
```bash
# Clone the repo
git clone https://github.com/ken-kaneki21/job-intelligence-pipeline.git
cd job-intelligence-pipeline

# Start infrastructure
docker compose up -d

# Run dbt transformations
cd job_intelligence
dbt run
dbt test

# Launch dashboard
streamlit run dashboard/app.py
```

## Pipeline DAG

`scrape_jobs` → `process_jobs` (scheduled daily at 9AM)
