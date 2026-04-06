import requests
import psycopg2
from datetime import datetime
import time

def get_db_connection():
    return psycopg2.connect(
        host="postgres",
        port=5432,
        database="job_pipeline",
        user="saurabh",
        password="password123"
    )

def fetch_jobs_from_api(keyword, location="Bangalore, India"):
    RAPIDAPI_KEY = "de2fb04390msh0e2f1b13965fb1cp1ea271jsn23b006f3279a"
    
    url = "https://jsearch.p.rapidapi.com/search"
    headers = {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
    }
    params = {
        "query": f"{keyword} in {location}",
        "page": "1",
        "num_results": "10"
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        print(f"Status: {response.status_code}")
        
        data = response.json()
        
        # Debug — see exactly what API returns
        print(f"Response keys: {list(data.keys())}")
        print(f"Status field: {data.get('status')}")
        print(f"Data count: {len(data.get('data', []))}")
        
        # Print first result raw if any
        if data.get('data'):
            print(f"First job keys: {list(data['data'][0].keys())}")
            first = data['data'][0]
            print(f"Sample - Title: {first.get('job_title')} | Company: {first.get('employer_name')}")
        else:
            print(f"Full response: {str(data)[:1000]}")
        
        jobs = []
        for job in data.get("data", []):
            jobs.append({
                "job_title": job.get("job_title", ""),
                "company_name": job.get("employer_name", ""),
                "location": job.get("job_city", location),
                "job_url": job.get("job_apply_link", ""),
                "source_platform": "JSearch"
            })
        
        print(f"Fetched {len(jobs)} jobs for '{keyword}'")
        return jobs
        
    except Exception as e:
        print(f"API error: {e}")
        try:
            print(f"Response text: {response.text[:500]}")
        except:
            pass
        return []

def get_mock_jobs(keyword):
    mock_jobs = [
        {
            "job_title": "Data Engineer",
            "company_name": "Razorpay",
            "location": "Bangalore",
            "job_url": "https://razorpay.com/jobs",
            "source_platform": "Mock"
        },
        {
            "job_title": "Senior Data Engineer",
            "company_name": "Swiggy",
            "location": "Bangalore",
            "job_url": "https://swiggy.com/careers",
            "source_platform": "Mock"
        },
        {
            "job_title": "Analytics Engineer",
            "company_name": "CRED",
            "location": "Bangalore",
            "job_url": "https://cred.club/careers",
            "source_platform": "Mock"
        },
        {
            "job_title": "ETL Developer",
            "company_name": "Meesho",
            "location": "Bangalore",
            "job_url": "https://meesho.com/careers",
            "source_platform": "Mock"
        },
        {
            "job_title": "Junior Data Engineer",
            "company_name": "Zepto",
            "location": "Bangalore",
            "job_url": "https://zepto.com/careers",
            "source_platform": "Mock"
        },
    ]
    print(f"Returning {len(mock_jobs)} mock jobs for '{keyword}'")
    return mock_jobs

def save_jobs_to_db(jobs):
    if not jobs:
        print("No jobs to save")
        return 0

    conn = get_db_connection()
    cur = conn.cursor()

    saved = 0
    for job in jobs:
        try:
            cur.execute("""
                INSERT INTO raw_jobs 
                    (job_title, company_name, location, job_url, source_platform)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                job["job_title"],
                job["company_name"],
                job["location"],
                job["job_url"],
                job["source_platform"]
            ))
            saved += 1
        except Exception as e:
            print(f"Error saving: {e}")

    conn.commit()
    cur.close()
    conn.close()
    print(f"Saved {saved} jobs to database")
    return saved

if __name__ == "__main__":
    keywords = ["data engineer", "analytics engineer", "ETL developer"]

    all_jobs = []
    for keyword in keywords:
        jobs = fetch_jobs_from_api(keyword)
        all_jobs.extend(jobs)
        time.sleep(1)

    save_jobs_to_db(all_jobs)
    print(f"Total: {len(all_jobs)} jobs")