from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "saurabh",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

def scrape_jobs():
    import sys
    sys.path.insert(0, '/opt/airflow/scripts')
    from scraper import fetch_jobs_from_api, save_jobs_to_db
    import time

    keywords = ["data engineer", "analytics engineer", "ETL developer"]
    all_jobs = []

    for keyword in keywords:
        jobs = fetch_jobs_from_api(keyword)
        all_jobs.extend(jobs)
        time.sleep(1)

    saved = save_jobs_to_db(all_jobs)
    print(f"Total jobs scraped and saved: {saved}")
    return saved

def process_jobs():
    print("Processing step — ATS scoring coming next!")

with DAG(
    dag_id="job_intelligence_pipeline",
    default_args=default_args,
    description="Scrape and process job listings daily",
    schedule_interval="0 9 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["job-pipeline", "de-project"],
) as dag:

    scrape_task = PythonOperator(
        task_id="scrape_jobs",
        python_callable=scrape_jobs,
    )

    process_task = PythonOperator(
        task_id="process_jobs",
        python_callable=process_jobs,
    )

    scrape_task >> process_task