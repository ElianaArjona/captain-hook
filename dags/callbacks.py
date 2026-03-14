"""
Shared on_failure_callback used by all sample failing DAGs.

When any DAG fails, this callback triggers the report generator DAG via the
Airflow REST API. The report agent then queries ALL recently failed DAGs,
so a single failure triggers a consolidated report covering everything.

max_active_runs=1 on the report DAG prevents duplicate reports when many
DAGs fail at the same time.
"""

import os
import logging

log = logging.getLogger(__name__)

AIRFLOW_API_URL = os.getenv("AIRFLOW_API_URL", "http://localhost:8080")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")
REPORT_DAG_ID = "dag_failure_report"


def _get_token(session) -> str:
    """Get a JWT Bearer token from Airflow's auth endpoint."""
    resp = session.post(
        f"{AIRFLOW_API_URL}/auth/token",
        json={"username": AIRFLOW_USERNAME, "password": AIRFLOW_PASSWORD},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def trigger_report_on_failure(context: dict) -> None:
    """
    Triggers the report generator DAG when any DAG fails.

    The report agent will look at ALL failed DAGs in the last hour,
    not just the one that triggered this callback.
    """
    import requests

    failed_dag_id = context["dag"].dag_id
    failed_run_id = context.get("run_id", "unknown")
    failed_at = str(context.get("logical_date", "unknown"))

    log.info(
        "DAG '%s' failed (run_id=%s). Triggering report DAG.",
        failed_dag_id,
        failed_run_id,
    )

    try:
        session = requests.Session()
        token = _get_token(session)
        session.headers["Authorization"] = f"Bearer {token}"

        from datetime import datetime, timezone
        # Round to the nearest 30-minute window so multiple DAG failures
        # within the same window all produce the same logical_date → 409 Conflict
        # on duplicates, ensuring only one report is generated per window.
        now = datetime.now(timezone.utc)
        window = now.replace(minute=(now.minute // 30) * 30, second=0, microsecond=0)

        response = session.post(
            f"{AIRFLOW_API_URL}/api/v2/dags/{REPORT_DAG_ID}/dagRuns",
            json={
                "logical_date": window.isoformat(),
                "conf": {
                    "triggered_by_dag": failed_dag_id,
                    "triggered_by_run": failed_run_id,
                    "triggered_at": failed_at,
                },
            },
            timeout=10,
        )

        if response.status_code in (200, 409):
            # 200 = triggered, 409 = already running (max_active_runs=1), both are fine
            log.info("Report DAG trigger response: %s", response.status_code)
        else:
            log.warning(
                "Unexpected response triggering report DAG: %s %s",
                response.status_code,
                response.text,
            )
    except Exception as exc:
        # Never let a callback failure affect the DAG state
        log.error("Failed to trigger report DAG: %s", exc)
