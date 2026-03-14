"""
Report Generator DAG

Runs every 30 minutes AND can be triggered by any failing DAG's on_failure_callback.

- max_active_runs=1 prevents duplicate reports when many DAGs fail simultaneously.
  If this DAG is already running when another callback fires, the new trigger
  is simply ignored (409 Conflict from the API).
- The report agent always looks at ALL failed DAGs in the last REPORT_LOOKBACK_HOURS,
  regardless of which DAG triggered the report.
"""

import sys
from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="dag_failure_report",
    description="Generates a consolidated markdown report of all DAG failures",
    schedule="*/30 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["reporting", "monitoring"],
    params={
        "triggered_by_dag": "",
        "triggered_by_run": "",
        "triggered_at": "",
    },
)
def dag_failure_report():
    @task()
    def generate_failure_report(**context) -> str:
        """
        Calls the report agent which uses Claude + Airflow MCP to analyze
        all recent failures and produce a markdown report.
        """
        # Make the agent module importable from /opt/airflow/agent
        if "/opt/airflow/agent" not in sys.path:
            sys.path.insert(0, "/opt/airflow/agent")

        from report_agent import run_report  # noqa: PLC0415

        triggered_by = context["params"].get("triggered_by_dag") or "scheduled"
        report_path = run_report(triggered_by=triggered_by)

        print(f"Report saved: {report_path}")
        return report_path

    generate_failure_report()


dag_failure_report()
