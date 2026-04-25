from airflow.decorators import dag, task
from datetime import datetime
import requests
import re
from bs4 import BeautifulSoup
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    dag_id="wuzzuf_etl_pipeline_regex",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
)
def pipeline():

    # 🔥 SCRAPE (IMPROVED REGEX + FILTERING)
    @task
    def scrape_jobs():
        url = "https://wuzzuf.net/search/jobs?q=Data%20Engineer&a=spbg"
        headers = {"User-Agent": "Mozilla/5.0"}

        html = requests.get(url, headers=headers).text
        soup = BeautifulSoup(html, "html.parser")

        jobs = []

        # Better filtering instead of scanning all divs blindly
        blocks = soup.find_all("div")

        for block in blocks:

            text = block.get_text(" ", strip=True)

            # skip noise blocks
            if len(text) < 100:
                continue

            # 🔥 Regex extraction
            title_match = re.search(
                r"(Senior|Junior|Lead|Data|Engineer|Developer|Analyst)[A-Za-z0-9\s\-]{5,120}",
                text
            )

            company_match = re.search(r"at\s([A-Za-z0-9\s&.,-]+)", text)
            location_match = re.search(r"(Cairo|Giza|Alex|Remote)[^\n]*", text)

            link_tag = block.find("a", href=True)
            job_link = "https://wuzzuf.net" + link_tag["href"] if link_tag else None

            # 🔥 VALIDATION
            if title_match and job_link and "jobs" in job_link:

                jobs.append({
                    "title": title_match.group(0),
                    "company": company_match.group(1).strip() if company_match else "Unknown",
                    "location": location_match.group(0) if location_match else "Unknown",
                    "job_link": job_link,
                    "job_types": "Data Engineer"
                })

        print("SCRAPED JOBS:", len(jobs))  # DEBUG

        return jobs


    # 🔥 VALIDATE
    @task
    def validate(jobs):
        return [
            j for j in jobs
            if j["title"] and j["job_link"]
        ]


    # 🔥 DEDUPLICATE
    @task
    def deduplicate(jobs):
        seen = set()
        unique = []

        for j in jobs:
            if j["job_link"] in seen:
                continue
            seen.add(j["job_link"])
            unique.append(j)

        return unique


    # 🔥 LOAD TO POSTGRES (FIXED + SAFE)
    @task
    def load_to_postgres(jobs):
        hook = PostgresHook(postgres_conn_id="postgres_conn")
        conn = hook.get_conn()
        cursor = conn.cursor()

        for job in jobs:
            cursor.execute("""
                INSERT INTO jobs (title, company, location, job_link, job_types)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (job_link) DO NOTHING;
            """, (
                job["title"],
                job["company"],
                job["location"],
                job["job_link"],
                job["job_types"]
            ))

        conn.commit()
        cursor.close()
        conn.close()


    # 🔗 PIPELINE FLOW
    raw = scrape_jobs()
    validated = validate(raw)
    clean = deduplicate(validated)
    load_to_postgres(clean)


pipeline()