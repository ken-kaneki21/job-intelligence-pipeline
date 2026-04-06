import os
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup
import psycopg2
import time
import hashlib
load_dotenv()

# ── SEARCH CONFIG — change these anytime ──────────────────
KEYWORDS = [
    "data engineer",
    "analytics engineer",
    "ETL developer",
    "data pipeline engineer"
]

LOCATIONS = [
    "Bangalore",
    "Hyderabad",
    "Mumbai",
    "Pune",
    "Remote"
]

# ── DB CONNECTION ──────────────────────────────────────────
def get_db_connection():
    return psycopg2.connect(
        host="postgres",
        port=5432,
        database="job_pipeline",
        user="saurabh",
        password="password123"
    )

# ── DUPLICATE DETECTION ────────────────────────────────────
def generate_job_hash(title, company, location):
    raw = f"{title.lower().strip()}{company.lower().strip()}{location.lower().strip()}"
    return hashlib.md5(raw.encode()).hexdigest()

# ── JSEARCH API (LinkedIn/Indeed/Glassdoor) ────────────────
def fetch_jsearch_jobs(keyword, location="Bangalore", pages=3):
    RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "30d1c29002msh1973ad3f364c39ap16a693jsn758b7e9a264e")
    url = "https://jsearch.p.rapidapi.com/search"
    headers = {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
    }

    jobs = []
    for page in range(1, pages + 1):
        try:
            params = {
                "query": f"{keyword} in {location}, India",
                "page": str(page),
                "num_results": "10"
            }
            response = requests.get(url, headers=headers, params=params, timeout=15)
            data = response.json()

            if data.get("status") != "OK":
                print(f"JSearch page {page} failed: {data.get('message')}")
                break

            page_jobs = data.get("data", [])
            if not page_jobs:
                break

            for job in page_jobs:
                jobs.append({
                    "job_title": job.get("job_title", ""),
                    "company_name": job.get("employer_name", ""),
                    "location": job.get("job_city", location),
                    "job_url": job.get("job_apply_link", ""),
                    "source_platform": "JSearch",
                    "job_description": job.get("job_description", "")[:1000],
                    "date_posted": job.get("job_posted_at_datetime_utc", None),
                    "job_hash": generate_job_hash(
                        job.get("job_title", ""),
                        job.get("employer_name", ""),
                        job.get("job_city", location)
                    )
                })

            print(f"JSearch page {page}: fetched {len(page_jobs)} jobs for '{keyword}' in '{location}'")
            time.sleep(1)

        except Exception as e:
            print(f"JSearch error page {page}: {e}")
            break

    print(f"JSearch total: {len(jobs)} jobs for '{keyword}' in '{location}'")
    return jobs


# ── NAUKRI SCRAPER ─────────────────────────────────────────
def scrape_naukri(keyword, location="bangalore"):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.naukri.com/",
        "appid": "109",
        "systemid": "109",
        "gid": "LOCATION,INDUSTRY,EDUCATION,FAREA_ROLE",
        "Connection": "keep-alive",
    }

    jobs = []
    keyword_url = keyword.replace(" ", "-")
    location_url = location.replace(" ", "-").lower()
    location_lower = location.lower()

    url = (
        f"https://www.naukri.com/jobapi/v3/search"
        f"?noOfResults=50&urlType=search_by_keyword&searchType=adv"
        f"&keyword={keyword.replace(' ', '%20')}"
        f"&location={location_lower}"
        f"&pageNo=1"
        f"&k={keyword.replace(' ', '%20')}"
        f"&l={location_lower}"
        f"&seoKey={keyword_url}-jobs-in-{location_url}"
        f"&src=jobsearchDesk&latLong="
    )

    try:
        response = requests.get(url, headers=headers, timeout=15)
        print(f"Naukri status: {response.status_code} for '{keyword}' in '{location}'")

        if response.status_code == 200:
            data = response.json()
            job_list = data.get("jobDetails", [])
            print(f"Naukri found {len(job_list)} jobs for '{keyword}' in '{location}'")

            for job in job_list:
                title = job.get("title", "")
                company = job.get("companyName", "")
                loc = job.get("placeholders", [{}])
                location_text = loc[0].get("label", location) if loc else location
                job_url = f"https://www.naukri.com{job.get('jdURL', '')}"

                if title and company:
                    jobs.append({
                        "job_title": title,
                        "company_name": company,
                        "location": location_text,
                        "job_url": job_url,
                        "source_platform": "Naukri",
                        "job_description": job.get("jobDescription", "")[:1000],
                        "date_posted": None,
                        "job_hash": generate_job_hash(title, company, location_text)
                    })

    except Exception as e:
        print(f"Naukri error: {e}")

    print(f"Naukri total: {len(jobs)} jobs for '{keyword}' in '{location}'")
    return jobs


# ── FOUNDIT SCRAPER ────────────────────────────────────────
def scrape_foundit(keyword, location="bangalore"):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    jobs = []
    keyword_url = keyword.replace(" ", "-")
    location_url = location.replace(" ", "-").lower()
    url = f"https://www.foundit.in/srp/results?query={keyword.replace(' ', '+')}&location={location}"

    try:
        response = requests.get(url, headers=headers, timeout=15)
        print(f"Foundit status: {response.status_code} for '{keyword}' in '{location}'")
        soup = BeautifulSoup(response.content, "html.parser")

        job_cards = (
            soup.find_all("div", class_="srpResultCardContainer") or
            soup.find_all("div", class_="jobcard") or
            soup.find_all("div", attrs={"data-job-id": True})
        )
        print(f"Foundit found {len(job_cards)} cards for '{keyword}' in '{location}'")

        for card in job_cards[:30]:
            try:
                title_el = (
                    card.find("h3") or
                    card.find("a", class_="jobTitle") or
                    card.find("div", class_="title")
                )
                company_el = (
                    card.find("div", class_="companyName") or
                    card.find("span", class_="company") or
                    card.find("a", class_="company-name")
                )
                link_el = card.find("a", href=True)

                title = title_el.text.strip() if title_el else ""
                company = company_el.text.strip() if company_el else ""
                link = f"https://www.foundit.in{link_el['href']}" if link_el else ""

                if title and company:
                    jobs.append({
                        "job_title": title,
                        "company_name": company,
                        "location": location,
                        "job_url": link,
                        "source_platform": "Foundit",
                        "job_description": "",
                        "date_posted": None,
                        "job_hash": generate_job_hash(title, company, location)
                    })
            except Exception as e:
                continue

    except Exception as e:
        print(f"Foundit error: {e}")

    print(f"Foundit total: {len(jobs)} jobs for '{keyword}' in '{location}'")
    return jobs


# ── INTERNSHALA SCRAPER ────────────────────────────────────
def scrape_internshala(keyword, location="bangalore"):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    jobs = []
    keyword_url = keyword.replace(" ", "-")
    location_url = location.replace(" ", "-").lower()
    url = f"https://internshala.com/jobs/{keyword_url}-jobs-in-{location_url}"

    try:
        response = requests.get(url, headers=headers, timeout=15)
        print(f"Internshala status: {response.status_code} for '{keyword}' in '{location}'")
        soup = BeautifulSoup(response.content, "html.parser")

        # Try multiple selectors
        job_cards = (
            soup.find_all("div", class_="individual_internship") or
            soup.find_all("div", class_="internship_meta") or
            soup.find_all("div", attrs={"data-internship_id": True}) or
            soup.find_all("div", class_="container-fluid")
        )
        print(f"Internshala found {len(job_cards)} cards for '{keyword}' in '{location}'")

        # Debug first card
        if job_cards:
            print(f"First card classes: {job_cards[0].get('class')}")
            print(f"First card snippet: {str(job_cards[0])[:300]}")

        for card in job_cards[:30]:
            try:
                # Try multiple title selectors
                title_el = (
                    card.find("h3", class_="job-internship-name") or
                    card.find("h3") or
                    card.find("a", class_="job-title-href") or
                    card.find("p", class_="profile")
                )
                # Try multiple company selectors
                company_el = (
                    card.find("p", class_="company-name") or
                    card.find("a", class_="link-unstyled") or
                    card.find("p", class_="company_name")
                )
                link_el = (
                    card.find("a", class_="job-title-href") or
                    card.find("a", href=True)
                )

                title = title_el.text.strip() if title_el else ""
                company = company_el.text.strip() if company_el else ""
                link = f"https://internshala.com{link_el['href']}" if link_el and link_el.get('href') else ""

                if title and company:
                    jobs.append({
                        "job_title": title,
                        "company_name": company,
                        "location": location,
                        "job_url": link,
                        "source_platform": "Internshala",
                        "job_description": "",
                        "date_posted": None,
                        "job_hash": generate_job_hash(title, company, location)
                    })
            except Exception as e:
                continue

    except Exception as e:
        print(f"Internshala error: {e}")

    print(f"Internshala total: {len(jobs)} jobs for '{keyword}' in '{location}'")
    return jobs

# ── SAVE TO DB WITH DEDUP ──────────────────────────────────
def save_jobs_to_db(jobs):
    if not jobs:
        print("No jobs to save")
        return 0

    conn = get_db_connection()
    cur = conn.cursor()

    saved = 0
    skipped = 0
    for job in jobs:
        try:
            cur.execute(
                "SELECT id FROM raw_jobs WHERE job_hash = %s",
                (job.get("job_hash", ""),)
            )
            if cur.fetchone():
                skipped += 1
                continue

            cur.execute("""
                INSERT INTO raw_jobs
                    (job_title, company_name, location, job_url,
                     source_platform, job_description, date_posted, job_hash)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                job["job_title"],
                job["company_name"],
                job["location"],
                job["job_url"],
                job["source_platform"],
                job.get("job_description", ""),
                job.get("date_posted"),
                job.get("job_hash", "")
            ))
            saved += 1

        except Exception as e:
            print(f"Error saving job: {e}")

    conn.commit()
    cur.close()
    conn.close()
    print(f"Saved: {saved} | Skipped duplicates: {skipped}")
    return saved


# ── MAIN ───────────────────────────────────────────────────
if __name__ == "__main__":
    all_jobs = []

    for keyword in KEYWORDS:
        for location in LOCATIONS:
            print(f"\n--- Scraping: '{keyword}' in '{location}' ---")

            all_jobs.extend(fetch_jsearch_jobs(keyword, location, pages=3))
            time.sleep(2)

            all_jobs.extend(scrape_naukri(keyword, location))
            time.sleep(2)

            all_jobs.extend(scrape_foundit(keyword, location))
            time.sleep(2)

            all_jobs.extend(scrape_internshala(keyword, location))
            time.sleep(2)

    save_jobs_to_db(all_jobs)
    print(f"\nTotal jobs processed: {len(all_jobs)}")