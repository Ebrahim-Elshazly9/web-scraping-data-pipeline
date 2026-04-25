from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="postgres_setup_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,   # run once manually or on demand
    catchup=False
) as dag:

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres_conn",
        sql="""
        CREATE TABLE IF NOT EXISTS jobs (
            id SERIAL PRIMARY KEY,
            title TEXT,
            company TEXT,
            location TEXT,
            job_link TEXT UNIQUE,
            job_types TEXT
        );  
        """
    )