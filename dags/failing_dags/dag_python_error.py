"""
Failure mode: Unexpected Python exception during data processing.
Simulates a KeyError when accessing a field that doesn't exist in the payload.
Common when upstream schema changes without notice.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from callbacks import trigger_report_on_failure


@dag(
    dag_id="failing_python_error",
    description="Simulates an unexpected KeyError during data processing",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["failing", "data-processing"],
    on_failure_callback=trigger_report_on_failure,
    default_args={"retries": 0},
)
def failing_python_error():
    @task()
    def fetch_event_payload() -> dict:
        # Simulates an API response that is missing an expected field
        return {
            "event_type": "user_signup",
            "timestamp": "2025-01-15T10:00:00Z",
            # NOTE: 'user_id' field is intentionally missing
        }

    @task()
    def process_event(payload: dict) -> dict:
        # This will raise KeyError because 'user_id' is not in the payload
        user_id = payload["user_id"]
        return {
            "user_id": user_id,
            "event": payload["event_type"],
            "processed_at": datetime.utcnow().isoformat(),
        }

    @task()
    def store_event(event: dict):
        print(f"Storing event for user {event['user_id']}")

    payload = fetch_event_payload()
    processed = process_event(payload)
    store_event(processed)


failing_python_error()
