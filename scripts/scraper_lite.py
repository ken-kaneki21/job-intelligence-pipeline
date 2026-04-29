"""
Lite scraper for GitHub Actions — JSearch only.
No ScraperAPI credits burned.
"""
from scraper import fetch_jsearch_jobs, scrape_internshala, \
    save_jobs_to_db, DEFAULT_KEYWORDS, DEFAULT_LOCATIONS
import time

def scrape_lite(user_id=1):
    all_jobs = []
    for keyword in DEFAULT_KEYWORDS:
        for location in DEFAULT_LOCATIONS:
            print(f"\n--- '{keyword}' in '{location}' ---")
            all_jobs.extend(
                fetch_jsearch_jobs(
                    keyword, location,
                    pages=2, user_id=user_id
                )
            )
            time.sleep(1)
            all_jobs.extend(
                scrape_internshala(
                    keyword, location, user_id
                )
            )
            time.sleep(1)

    return save_jobs_to_db(all_jobs, user_id)

if __name__ == "__main__":
    scrape_lite()
