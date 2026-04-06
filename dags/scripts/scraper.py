import requests
from bs4 import BeautifulSoup
import psycopg2
from datetime import datetime
import time

# Database connection
def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="job_pipeline",
        user="saurabh",
        password="password123"
    )

def scrape_naukri_jobs(keyword, location="Bangalore"):
    """
    Scrape job listings from Naukri
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    jobs = []
    url = f"https://www.naukri.com/{keyword.replace(' ', '-')}-jobs-in-{location.lower()}"
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.content, "html.parser")
        
        job_cards = soup.find_all("article", class_="jobTuple")
        
        for card in job_cards[:20]:  # limit to 20 per run
            try:
                title = card.find("a", class_="title")
                company = card.find("a", class_="subTitle")
                loc = card.find("li", class_="location")
                link = card.find("a", class_="title")
                
                if title and company:
                    jobs.append({
                        "job_title": title.text.strip(),
                        "company_name": company.text.strip(),
                        "location": loc.text.strip() if loc else location,
                        "job_url": link["href"] if link else "",
                        "source_platform": "Naukri"
                    })
            except Exception as e:
                print(f"Error parsing job card: {e}")
                continue
                
        print(f"Scraped {len(jobs)} jobs from Naukri")
        
    except Exception as e:
        print(f"Error scraping Naukri: {e}")
    
    return jobs

def save_jobs_to_db(jobs):
    """
    Save scraped jobs to PostgreSQL
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    saved = 0
    for job in jobs:
        try:
            cur.execute("""
                INSERT INTO raw_jobs 
                    (job_title, company_name, location, job_url, source_platform)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                job["job_title"],
                job["company_name"], 
                job["location"],
                job["job_url"],
                job["source_platform"]
            ))
            saved += 1
        except Exception as e:
            print(f"Error saving job: {e}")
            
    conn.commit()
    cur.close()
    conn.close()
    print(f"Saved {saved} jobs to database")

if __name__ == "__main__":
    keywords = ["data engineer", "analytics engineer", "ETL developer"]
    
    all_jobs = []
    for keyword in keywords:
        jobs = scrape_naukri_jobs(keyword)
        all_jobs.extend(jobs)
        time.sleep(2)  # be polite, don't hammer the server
    
    save_jobs_to_db(all_jobs)
    print(f"Total jobs scraped: {len(all_jobs)}")