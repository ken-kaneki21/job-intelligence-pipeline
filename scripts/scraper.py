import requests
from bs4 import BeautifulSoup
import psycopg2
import time
import hashlib
import os
import json
import re
from dotenv import load_dotenv

load_dotenv()

# ── CONFIG ─────────────────────────────────────────────────
DEFAULT_KEYWORDS = [
    "data engineer",
    "analytics engineer",
    "ETL developer",
    "data pipeline engineer",
    "data analyst",
]

DEFAULT_LOCATIONS = [
    "Bangalore",
    "Hyderabad",
    "Mumbai",
    "Pune",
    "Remote"
]

# ── DB CONNECTION ──────────────────────────────────────────
def get_db_connection():
    host = os.getenv("DB_HOST", "localhost")
    return psycopg2.connect(
        host=host,
        port=5432,
        database="job_pipeline",
        user="saurabh",
        password="password123"
    )

# ── DEDUP HASH ─────────────────────────────────────────────
def generate_job_hash(title, company, location):
    raw = (
        f"{(title or '').lower().strip()}"
        f"{(company or '').lower().strip()}"
        f"{(location or '').lower().strip()}"
    )
    return hashlib.md5(raw.encode()).hexdigest()

# ── SCRAPERAPI HELPER ──────────────────────────────────────
def fetch_via_scraperapi(url, render=False, country="in"):
    key = os.getenv("SCRAPERAPI_KEY", "")
    if not key:
        print("No SCRAPERAPI_KEY found in .env")
        return None
    try:
        resp = requests.get(
            "https://api.scraperapi.com",
            params={
                "api_key": key,
                "url": url,
                "render": str(render).lower(),
                "country_code": country,
                "premium": "true",
            },
            timeout=120,
        )
        if resp.status_code == 200:
            return resp
        else:
            print(f"ScraperAPI returned {resp.status_code}")
            return None
    except Exception as e:
        print(f"ScraperAPI error: {e}")
        return None

# ── JSEARCH API ────────────────────────────────────────────
def fetch_jsearch_jobs(keyword, location="Bangalore",
                       pages=3, user_id=1):
    key = os.getenv("RAPIDAPI_KEY", "")
    if not key:
        print("No RAPIDAPI_KEY found in .env")
        return []

    url = "https://jsearch.p.rapidapi.com/search"
    headers = {
        "X-RapidAPI-Key": key,
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
            response = requests.get(
                url, headers=headers,
                params=params, timeout=15
            )
            data = response.json()

            if data.get("status") != "OK":
                break

            page_jobs = data.get("data", [])
            if not page_jobs:
                break

            for job in page_jobs:
                title = job.get("job_title", "") or ""
                company = job.get("employer_name", "") or ""
                city = (
                    job.get("job_city", location) or location
                )

                jobs.append({
                    "job_title": title,
                    "company_name": company,
                    "location": city,
                    "job_url": (
                        job.get("job_apply_link", "") or ""
                    ),
                    "source_platform": "JSearch",
                    "job_description": (
                        job.get("job_description", "") or ""
                    )[:2000],
                    "date_posted": job.get(
                        "job_posted_at_datetime_utc"
                    ),
                    "user_id": user_id,
                    "job_hash": generate_job_hash(
                        title, company, city
                    )
                })

            print(f"JSearch page {page}: {len(page_jobs)} "
                  f"jobs for '{keyword}' in '{location}'")
            time.sleep(1)

        except Exception as e:
            print(f"JSearch error page {page}: {e}")
            break

    return jobs

# ── NAUKRI SCRAPER ─────────────────────────────────────────
def scrape_naukri(keyword, location="bangalore", user_id=1):
    jobs = []
    keyword_url = keyword.replace(" ", "-")
    location_url = location.lower().replace(" ", "-")
    url = (
        f"https://www.naukri.com/"
        f"{keyword_url}-jobs-in-{location_url}"
    )

    print(f"Naukri: '{keyword}' in '{location}'")
    resp = fetch_via_scraperapi(url, render=True, country="in")

    if not resp or resp.status_code != 200:
        print("Naukri: failed to fetch page")
        return []

    try:
        soup = BeautifulSoup(resp.content, "html.parser")
        text = resp.text
        pattern = r'"jobDetails"\s*:\s*(\[[\s\S]*?\])\s*,'
        match = re.search(pattern, text)

        if match:
            try:
                job_list = json.loads(match.group(1))
                print(f"Naukri JSON: {len(job_list)} jobs")
                for job in job_list[:30]:
                    title = job.get("title", "") or ""
                    company = job.get("companyName", "") or ""
                    jd_url = job.get("jdURL", "")
                    job_url = (
                        f"https://www.naukri.com{jd_url}"
                        if jd_url else ""
                    )
                    skills = job.get("tagsAndSkills", "") or ""
                    placeholders = job.get("placeholders", [])
                    loc_text = location
                    for p in (placeholders or []):
                        if p.get("type") == "location":
                            loc_text = p.get("label", location)
                            break
                    if title and company:
                        jobs.append({
                            "job_title": title,
                            "company_name": company,
                            "location": loc_text,
                            "job_url": job_url,
                            "source_platform": "Naukri",
                            "job_description": skills[:2000],
                            "date_posted": None,
                            "user_id": user_id,
                            "job_hash": generate_job_hash(
                                title, company, location
                            )
                        })
            except Exception as e:
                print(f"Naukri JSON parse error: {e}")

        if not jobs:
            print("Naukri: trying HTML cards...")
            job_cards = (
                soup.find_all(
                    "div", class_="srp-jobtuple-wrapper"
                ) or
                soup.find_all(attrs={"data-job-id": True})
            )
            print(f"Naukri HTML: {len(job_cards)} cards")

            for card in job_cards[:30]:
                try:
                    title_el = (
                        card.find("a", class_="title") or
                        card.find(
                            "a",
                            attrs={"class": lambda c:
                                   c and "title" in c}
                        ) or card.find("h2")
                    )
                    company_el = (
                        card.find("a", class_="comp-name") or
                        card.find("span", class_="comp-name")
                    )
                    title = (
                        title_el.text.strip()
                        if title_el else ""
                    )
                    company = (
                        company_el.text.strip()
                        if company_el else ""
                    )
                    job_url = ""
                    if title_el and title_el.get("href"):
                        job_url = title_el["href"]
                    skills_tags = (
                        card.find_all("li", class_="tag-li") or
                        card.find_all("span", class_="tag")
                    )
                    skills = " ".join([
                        t.text.strip() for t in skills_tags
                    ])
                    if title and company:
                        jobs.append({
                            "job_title": title,
                            "company_name": company,
                            "location": location,
                            "job_url": job_url,
                            "source_platform": "Naukri",
                            "job_description": skills,
                            "date_posted": None,
                            "user_id": user_id,
                            "job_hash": generate_job_hash(
                                title, company, location
                            )
                        })
                except:
                    continue

    except Exception as e:
        print(f"Naukri parse error: {e}")

    print(f"Naukri: {len(jobs)} jobs for "
          f"'{keyword}' in '{location}'")
    return jobs

# ── FOUNDIT SCRAPER ────────────────────────────────────────
def scrape_foundit(keyword, location="bangalore", user_id=1):
    jobs = []
    api_url = (
        f"https://www.foundit.in/middleware/jobsearch"
        f"?searchPhrase={keyword.replace(' ', '+')}"
        f"&locations={location}"
        f"&start=0&limit=30"
    )

    print(f"Foundit: '{keyword}' in '{location}'")
    resp = fetch_via_scraperapi(
        api_url, render=False, country="in"
    )

    if resp:
        try:
            data = resp.json()
            job_list = (
                data.get("jobSearchResponse", {})
                .get("data", {})
                .get("jobs", [])
            ) or data.get("jobs", []) or []

            for job in job_list:
                title = job.get("title", "") or ""
                company = (
                    job.get("companyName", "") or
                    job.get("company", "") or ""
                )
                loc = (
                    job.get("location", "") or
                    job.get("city", location) or location
                )
                job_url = (
                    job.get("applyRedirectUrl", "") or ""
                )
                if not job_url:
                    jid = job.get("jobId", "")
                    if jid:
                        job_url = (
                            f"https://www.foundit.in"
                            f"/job/{jid}"
                        )

                if title and company:
                    jobs.append({
                        "job_title": title,
                        "company_name": company,
                        "location": loc,
                        "job_url": job_url,
                        "source_platform": "Foundit",
                        "job_description": (
                            job.get(
                                "jobDescription", ""
                            ) or ""
                        )[:2000],
                        "date_posted": None,
                        "user_id": user_id,
                        "job_hash": generate_job_hash(
                            title, company, loc
                        )
                    })
        except Exception as e:
            print(f"Foundit API parse error: {e}")

    if not jobs:
        srp_url = (
            f"https://www.foundit.in/srp/results"
            f"?query={keyword.replace(' ', '+')}"
            f"&location={location}"
        )
        resp2 = fetch_via_scraperapi(
            srp_url, render=True, country="in"
        )
        if resp2:
            try:
                soup = BeautifulSoup(
                    resp2.content, "html.parser"
                )
                cards = (
                    soup.find_all(
                        "div",
                        class_="srpResultCardContainer"
                    ) or
                    soup.find_all(
                        "div",
                        attrs={"data-job-id": True}
                    )
                )
                for card in cards[:30]:
                    try:
                        title_el = (
                            card.find("h3") or
                            card.find(
                                "a", class_="jobTitle"
                            )
                        )
                        company_el = (
                            card.find(
                                "div",
                                class_="companyName"
                            ) or
                            card.find(
                                "span", class_="company"
                            )
                        )
                        link_el = card.find("a", href=True)
                        title = (
                            title_el.text.strip()
                            if title_el else ""
                        )
                        company = (
                            company_el.text.strip()
                            if company_el else ""
                        )
                        link = ""
                        if link_el and link_el.get("href"):
                            href = link_el["href"]
                            link = (
                                f"https://www.foundit.in"
                                f"{href}"
                                if href.startswith("/")
                                else href
                            )
                        if title and company:
                            jobs.append({
                                "job_title": title,
                                "company_name": company,
                                "location": location,
                                "job_url": link,
                                "source_platform": "Foundit",
                                "job_description": "",
                                "date_posted": None,
                                "user_id": user_id,
                                "job_hash": generate_job_hash(
                                    title, company, location
                                )
                            })
                    except:
                        continue
            except Exception as e:
                print(f"Foundit HTML parse error: {e}")

    print(f"Foundit: {len(jobs)} jobs for "
          f"'{keyword}' in '{location}'")
    return jobs

# ── INTERNSHALA SCRAPER ────────────────────────────────────
def scrape_internshala(keyword, location="bangalore",
                       user_id=1):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; "
                      "Win64; x64) AppleWebKit/537.36",
    }
    jobs = []
    keyword_url = keyword.replace(" ", "-")
    location_url = location.replace(" ", "-").lower()
    url = (
        f"https://internshala.com/jobs/"
        f"{keyword_url}-jobs-in-{location_url}"
    )

    try:
        response = requests.get(
            url, headers=headers, timeout=15
        )
        soup = BeautifulSoup(response.content, "html.parser")
        job_cards = (
            soup.find_all(
                "div", class_="individual_internship"
            ) or
            soup.find_all(
                "div",
                attrs={"data-internship_id": True}
            )
        )

        for card in job_cards[:30]:
            try:
                title_el = (
                    card.find(
                        "h3",
                        class_="job-internship-name"
                    ) or
                    card.find("h3") or
                    card.find("a", class_="job-title-href")
                )
                company_el = (
                    card.find("p", class_="company-name") or
                    card.find("a", class_="link-unstyled")
                )
                link_el = (
                    card.find(
                        "a", class_="job-title-href"
                    ) or
                    card.find("a", href=True)
                )
                title = (
                    title_el.text.strip()
                    if title_el else ""
                )
                company = (
                    company_el.text.strip()
                    if company_el else ""
                )
                link = ""
                if link_el and link_el.get("href"):
                    href = link_el["href"]
                    link = (
                        f"https://internshala.com{href}"
                        if href.startswith("/") else href
                    )
                if title and company:
                    jobs.append({
                        "job_title": title,
                        "company_name": company,
                        "location": location,
                        "job_url": link,
                        "source_platform": "Internshala",
                        "job_description": "",
                        "date_posted": None,
                        "user_id": user_id,
                        "job_hash": generate_job_hash(
                            title, company, location
                        )
                    })
            except:
                continue

    except Exception as e:
        print(f"Internshala error: {e}")

    print(f"Internshala: {len(jobs)} jobs for "
          f"'{keyword}' in '{location}'")
    return jobs

# ── GOOGLE JOBS SCRAPER ────────────────────────────────────
def scrape_google_jobs(keyword, location="Bangalore",
                       user_id=1):
    key = os.getenv("SERPER_KEY", "")
    if not key:
        print("No SERPER_KEY in .env")
        return []

    jobs = []
    try:
        response = requests.post(
            "https://google.serper.dev/search",
            headers={
                "X-API-KEY": key,
                "Content-Type": "application/json"
            },
            json={
                "q": f"{keyword} jobs {location} India",
                "gl": "in",
                "hl": "en",
                "num": 10
            },
            timeout=30
        )

        data = response.json()

        # Free tier returns organic search results
        organic = data.get("organic", [])

        for item in organic[:10]:
            title = item.get("title", "") or ""
            link = item.get("link", "") or ""
            snippet = item.get("snippet", "") or ""
            display = item.get("displayLink", "") or ""

            # Skip non-job pages
            skip_domains = [
                "youtube.com", "wikipedia.org",
                "reddit.com", "quora.com",
                "coursera.org", "udemy.com"
            ]
            if any(d in display for d in skip_domains):
                continue

            # Extract company from title
            company = ""
            title_lower = title.lower()
            if " at " in title_lower:
                parts = title.split(" at ")
                if len(parts) > 1:
                    company = parts[-1].strip()
            if not company:
                # Use domain as company fallback
                company = display.replace(
                    "www.", ""
                ).split(".")[0].title()

            # Detect platform from URL
            platform = "Google Jobs"
            if "naukri.com" in link:
                platform = "Naukri"
            elif "linkedin.com" in link:
                platform = "LinkedIn"
            elif "indeed.com" in link:
                platform = "Indeed"
            elif "foundit.in" in link:
                platform = "Foundit"
            elif "glassdoor" in link:
                platform = "Glassdoor"
            elif "wellfound.com" in link:
                platform = "Wellfound"
            elif "internshala.com" in link:
                platform = "Internshala"

            if title and company:
                jobs.append({
                    "job_title": title,
                    "company_name": company,
                    "location": location,
                    "job_url": link,
                    "source_platform": platform,
                    "job_description": snippet[:2000],
                    "date_posted": None,
                    "user_id": user_id,
                    "job_hash": generate_job_hash(
                        title, company, location
                    )
                })

        print(f"Google Jobs: {len(jobs)} jobs for "
              f"'{keyword}' in '{location}'")

    except Exception as e:
        print(f"Google Jobs error: {e}")

    return jobs

# ── SAVE TO DB ─────────────────────────────────────────────
def save_jobs_to_db(jobs, user_id=1):
    if not jobs:
        return 0

    conn = get_db_connection()
    cur = conn.cursor()
    saved = 0
    skipped = 0

    for job in jobs:
        try:
            cur.execute(
                "SELECT id FROM raw_jobs "
                "WHERE job_hash = %s AND user_id = %s",
                (job.get("job_hash", ""), user_id)
            )
            if cur.fetchone():
                skipped += 1
                continue

            cur.execute("""
                INSERT INTO raw_jobs
                    (job_title, company_name, location,
                     job_url, source_platform,
                     job_description, date_posted,
                     job_hash, user_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                job["job_title"],
                job["company_name"],
                job["location"],
                job["job_url"],
                job["source_platform"],
                job.get("job_description", ""),
                job.get("date_posted"),
                job.get("job_hash", ""),
                user_id
            ))
            saved += 1

        except Exception as e:
            print(f"Save error: {e}")
            conn.rollback()
            continue

    conn.commit()
    cur.close()
    conn.close()
    print(f"Saved: {saved} | Skipped: {skipped}")
    return saved

# ── KEYWORD EXTRACTION FROM RESUME ────────────────────────
def extract_search_keywords(resume_text, extracted_skills):
    resume_lower = resume_text.lower()
    role_map = {
        "data engineer": [
            "data engineer", "etl developer",
            "data pipeline engineer", "analytics engineer"
        ],
        "analytics engineer": [
            "analytics engineer", "data analyst"
        ],
        "data scientist": [
            "data scientist", "ml engineer"
        ],
        "product analyst": [
            "product analyst", "product data analyst"
        ],
        "business analyst": [
            "business analyst", "data analyst"
        ],
        "ml engineer": [
            "ml engineer", "machine learning engineer",
            "ai engineer"
        ],
    }

    detected = []
    for role, searches in role_map.items():
        if role in resume_lower:
            detected.extend(searches)

    keywords = (
        list(set(detected)) if detected
        else DEFAULT_KEYWORDS.copy()
    )

    skill_searches = {
        "airflow": "airflow data engineer",
        "dbt": "dbt analytics engineer",
        "spark": "spark data engineer",
        "kafka": "kafka data engineer",
        "snowflake": "snowflake data engineer",
        "langchain": "ai engineer llm",
        "pytorch": "ml engineer deep learning",
    }

    skills_lower = (extracted_skills or "").lower()
    for skill, search in skill_searches.items():
        if skill in skills_lower and search not in keywords:
            keywords.append(search)

    return keywords[:6]

def extract_location_from_resume(resume_text):
    resume_lower = resume_text.lower()
    city_map = {
        "bangalore": "Bangalore",
        "bengaluru": "Bangalore",
        "hyderabad": "Hyderabad",
        "mumbai": "Mumbai",
        "pune": "Pune",
        "delhi": "Delhi",
        "chennai": "Chennai",
        "noida": "Noida",
        "gurgaon": "Gurgaon",
        "gurugram": "Gurgaon",
    }
    detected = []
    for key, city in city_map.items():
        if key in resume_lower and city not in detected:
            detected.append(city)
    if "Remote" not in detected:
        detected.append("Remote")
    return (
        detected[:4] if len(detected) > 1
        else DEFAULT_LOCATIONS
    )

# ── RESUME-DRIVEN SCRAPE ───────────────────────────────────
def scrape_for_resume(resume_id, user_id=1):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT raw_text, extracted_skills "
        "FROM resumes WHERE id = %s",
        (resume_id,)
    )
    result = cur.fetchone()
    cur.close()
    conn.close()

    if not result:
        print(f"Resume {resume_id} not found")
        return 0

    resume_text, extracted_skills = result
    keywords = extract_search_keywords(
        resume_text, extracted_skills or ""
    )
    locations = extract_location_from_resume(resume_text)

    print(f"Resume keywords: {keywords}")
    print(f"Resume locations: {locations}")

    all_jobs = []
    for keyword in keywords:
        for location in locations:
            print(f"\n--- '{keyword}' in '{location}' ---")
            all_jobs.extend(
                fetch_jsearch_jobs(
                    keyword, location,
                    pages=2, user_id=user_id
                )
            )
            time.sleep(2)
            all_jobs.extend(
                scrape_naukri(keyword, location, user_id)
            )
            time.sleep(2)
            all_jobs.extend(
                scrape_internshala(
                    keyword, location, user_id
                )
            )
            time.sleep(2)
            all_jobs.extend(
                scrape_google_jobs(
                    keyword, location, user_id
                )
            )
            time.sleep(2)

    saved = save_jobs_to_db(all_jobs, user_id)
    print(f"\nTotal: {len(all_jobs)} found, {saved} new")
    return saved

# ── DEFAULT DAG SCRAPE ─────────────────────────────────────
def scrape_default(user_id=1):
    all_jobs = []
    for keyword in DEFAULT_KEYWORDS:
        for location in DEFAULT_LOCATIONS:
            print(f"\n--- '{keyword}' in '{location}' ---")
            all_jobs.extend(
                fetch_jsearch_jobs(
                    keyword, location,
                    pages=3, user_id=user_id
                )
            )
            time.sleep(2)
            all_jobs.extend(
                scrape_naukri(keyword, location, user_id)
            )
            time.sleep(2)
            all_jobs.extend(
                scrape_foundit(keyword, location, user_id)
            )
            time.sleep(2)
            all_jobs.extend(
                scrape_internshala(
                    keyword, location, user_id
                )
            )
            time.sleep(2)
            all_jobs.extend(
                scrape_google_jobs(
                    keyword, location, user_id
                )
            )
            time.sleep(2)

    return save_jobs_to_db(all_jobs, user_id)

if __name__ == "__main__":
    scrape_default()