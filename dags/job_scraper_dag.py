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
    from scraper import scrape_default
    saved = scrape_default(user_id=1)
    print(f"Daily scrape complete: {saved} new jobs")
    return saved

def process_jobs():
    import sys
    sys.path.insert(0, '/opt/airflow/scripts')
    from resume_matcher import score_jobs_for_resume
    import psycopg2

    # Score jobs for all existing resumes
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database="job_pipeline",
        user="saurabh",
        password="password123"
    )
    cur = conn.cursor()
    cur.execute("SELECT id FROM resumes ORDER BY id")
    resume_ids = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()

    total_scored = 0
    for resume_id in resume_ids:
        print(f"Scoring jobs for resume {resume_id}...")
        scored = score_jobs_for_resume(resume_id)
        total_scored += scored

    print(f"Process complete: {total_scored} jobs scored across {len(resume_ids)} resumes")
    return total_scored

with DAG(
    dag_id="job_intelligence_pipeline",
    default_args=default_args,
    description="Daily job scraping + resume scoring pipeline",
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