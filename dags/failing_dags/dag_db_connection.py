"""
Failure mode: Database connection error.
Simulates a pipeline that tries to connect to a non-existent PostgreSQL database.
Common in prod when credentials rotate or a DB host goes down.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from callbacks import trigger_report_on_failure


@dag(
    dag_id="failing_db_connection",
    description="Simulates a broken database connection",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["failing", "database"],
    on_failure_callback=trigger_report_on_failure,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=10)},
)
def failing_db_connection():
    @task()
    def extract_from_db():
        import psycopg2

        try:
            conn = psycopg2.connect(
                host="non-existent-db-host",
                port=5432,
                dbname="analytics",
                user="pipeline_user",
                password="secret",
                connect_timeout=5,
            )
            conn.close()
        except Exception as exc:
            raise AirflowException(
                f"Cannot connect to analytics DB at non-existent-db-host:5432. "
                f"Original error: {exc}"
            ) from exc

    @task()
    def transform(data: dict):
        return {"records": data.get("count", 0) * 2}

    @task()
    def load(data: dict):
        print(f"Loading {data['records']} records to warehouse")

    raw = extract_from_db()
    transformed = transform(raw)
    load(transformed)


failing_db_connection()
