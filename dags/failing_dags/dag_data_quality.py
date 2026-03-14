"""
Failure mode: Data quality check fails.
Simulates a pipeline that loads data and then validates it meets quality thresholds.
The check finds anomalies and intentionally stops the pipeline to avoid
propagating bad data downstream.

This pattern is critical: if we let bad data through, it silently corrupts
dashboards and downstream models.
"""

from datetime import datetime

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from callbacks import trigger_report_on_failure


@dag(
    dag_id="failing_data_quality",
    description="Simulates a data quality check that fails due to anomalous data",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["failing", "data-quality"],
    on_failure_callback=trigger_report_on_failure,
    default_args={"retries": 0},
)
def failing_data_quality():
    @task()
    def ingest_daily_sales() -> dict:
        # Simulates ingesting today's sales data
        return {
            "date": "2025-01-15",
            "total_revenue": 85.50,       # Suspiciously low (normally ~$50,000)
            "order_count": 3,             # Way too few (normally ~800)
            "null_customer_ids": 2,       # There should be zero nulls
            "duplicate_order_ids": 5,     # Should always be 0
        }

    @task()
    def run_quality_checks(data: dict) -> dict:
        errors = []

        if data["total_revenue"] < 1_000:
            errors.append(
                f"Revenue anomaly: ${data['total_revenue']:.2f} is below $1,000 "
                f"minimum threshold (expected ~$50,000)"
            )

        if data["order_count"] < 100:
            errors.append(
                f"Order count anomaly: {data['order_count']} orders "
                f"(expected >= 100, normally ~800)"
            )

        if data["null_customer_ids"] > 0:
            errors.append(
                f"Data integrity: {data['null_customer_ids']} NULL customer_ids found"
            )

        if data["duplicate_order_ids"] > 0:
            errors.append(
                f"Data integrity: {data['duplicate_order_ids']} duplicate order_ids found"
            )

        if errors:
            raise AirflowException(
                f"Data quality checks FAILED for {data['date']}. "
                f"{len(errors)} issue(s) detected:\n"
                + "\n".join(f"  - {e}" for e in errors)
            )

        return data

    @task()
    def load_to_warehouse(data: dict):
        print(f"Loading validated sales data for {data['date']} to warehouse")

    @task()
    def update_sales_dashboard():
        print("Refreshing sales dashboard")

    raw = ingest_daily_sales()
    validated = run_quality_checks(raw)
    loaded = load_to_warehouse(validated)
    update_sales_dashboard()

    loaded >> update_sales_dashboard()


failing_data_quality()
