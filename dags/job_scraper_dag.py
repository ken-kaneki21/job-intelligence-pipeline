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
    from scraper import (fetch_jsearch_jobs, scrape_naukri,
                         scrape_foundit, scrape_internshala,
                         save_jobs_to_db, KEYWORDS, LOCATIONS)
    import time

    all_jobs = []
    for keyword in KEYWORDS:
        for location in LOCATIONS:
            print(f"\n--- '{keyword}' in '{location}' ---")
            all_jobs.extend(fetch_jsearch_jobs(keyword, location, pages=3))
            time.sleep(2)
            all_jobs.extend(scrape_naukri(keyword, location))
            time.sleep(2)
            all_jobs.extend(scrape_foundit(keyword, location))
            time.sleep(2)
            all_jobs.extend(scrape_internshala(keyword, location))
            time.sleep(2)

    saved = save_jobs_to_db(all_jobs)
    print(f"Total: {len(all_jobs)} found, {saved} new saved")
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