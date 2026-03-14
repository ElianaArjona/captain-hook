"""
Failure mode: Upstream dependency never completes (sensor timeout).
Simulates a DAG that waits for a partner DAG/file that never arrives.
Common when an upstream team's pipeline is delayed or broken.

This also demonstrates downstream impact: if this DAG never completes,
any DAG waiting on it will also be stuck.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from callbacks import trigger_report_on_failure


@dag(
    dag_id="failing_upstream_sensor",
    description="Simulates a sensor timing out waiting for an upstream dependency",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["failing", "sensor", "dependency"],
    on_failure_callback=trigger_report_on_failure,
    default_args={"retries": 0},
)
def failing_upstream_sensor():
    # Waits for a file that will never appear (upstream feed is broken)
    wait_for_upstream_file = FileSensor(
        task_id="wait_for_partner_feed",
        filepath="/opt/airflow/data/partner_feed_{{ ds }}.csv",
        timeout=30,  # seconds — will timeout quickly in POC
        poke_interval=10,
        mode="poke",
        soft_fail=False,
    )

    @task()
    def process_partner_data():
        print("Processing partner feed data...")
        return {"records": 1200}

    @task()
    def generate_report(data: dict):
        print(f"Generating report for {data['records']} partner records")

    processed = process_partner_data()
    generate_report(processed)

    wait_for_upstream_file >> processed


failing_upstream_sensor()
