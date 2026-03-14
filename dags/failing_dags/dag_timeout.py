"""
Failure mode: Task execution timeout.
Simulates a long-running ML training job that exceeds the allowed execution window.
Common when data grows larger than expected or a remote API becomes slow.
"""

import time
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowTaskTimeout
from callbacks import trigger_report_on_failure


@dag(
    dag_id="failing_timeout",
    description="Simulates a task that exceeds its execution_timeout",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["failing", "ml-training"],
    on_failure_callback=trigger_report_on_failure,
    default_args={"retries": 0},
)
def failing_timeout():
    @task()
    def prepare_dataset() -> dict:
        print("Preparing training dataset...")
        return {"rows": 500_000, "features": 128}

    @task(execution_timeout=timedelta(seconds=10))
    def train_model(dataset: dict):
        """
        Pretends to train a model but takes way longer than the 10s timeout.
        Simulates a job that used to complete in 8s but now takes 45s because
        the dataset doubled in size.
        """
        print(f"Starting model training on {dataset['rows']} rows...")
        # Sleeping 45s will cause AirflowTaskTimeout after 10s
        time.sleep(45)
        print("Training complete.")  # Never reached

    @task()
    def publish_model():
        print("Publishing model to registry")

    dataset = prepare_dataset()
    model = train_model(dataset)
    publish_model()


failing_timeout()
