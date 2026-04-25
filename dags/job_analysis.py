from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json

@dag(
    dag_id="jobs_analytics",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
)
def analytics_pipeline():

    # 🔥 EXTRACT
    @task
    def extract():
        hook = PostgresHook(postgres_conn_id="postgres_conn")
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("SELECT title, company, location FROM jobs")
        rows = cursor.fetchall()

        cursor.close()
        conn.close()

        return rows

    # 🔥 TRANSFORM
    @task
    def transform(rows):
        companies = {}
        locations = {}

        for title, company, location in rows:
            companies[company] = companies.get(company, 0) + 1
            locations[location] = locations.get(location, 0) + 1

        return {
            "total_jobs": len(rows),
            "top_companies": dict(sorted(companies.items(), key=lambda x: x[1], reverse=True)[:5]),
            "top_locations": dict(sorted(locations.items(), key=lambda x: x[1], reverse=True)[:5])
        }

    # 🔥 LOAD (SAVE BACK TO POSTGRES)
    @task
    def load(summary):
        hook = PostgresHook(postgres_conn_id="postgres_conn")
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs_summary (
                id SERIAL PRIMARY KEY,
                run_date TIMESTAMP DEFAULT NOW(),
                total_jobs INT,
                top_companies JSONB,
                top_locations JSONB
            );
        """)

        cursor.execute("""
            INSERT INTO jobs_summary (total_jobs, top_companies, top_locations)
            VALUES (%s, %s, %s);
        """, (
            summary["total_jobs"],
            json.dumps(summary["top_companies"]),
            json.dumps(summary["top_locations"])
        ))

        conn.commit()
        cursor.close()
        conn.close()

    raw = extract()
    summary = transform(raw)
    load(summary)


analytics_pipeline()