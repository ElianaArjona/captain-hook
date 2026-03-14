"""
Configure the astro-airflow-mcp adapter with Airflow credentials at startup.

When astro-airflow-mcp runs as an Airflow plugin (HTTP mode), it initializes
without credentials. This plugin injects them from environment variables so
the MCP tools can authenticate against the Airflow REST API.
"""

import os

from airflow.plugins_manager import AirflowPlugin

try:
    from astro_airflow_mcp.server import _manager

    _manager.configure(
        url=os.getenv("AIRFLOW_API_URL", "http://localhost:8080"),
        username=os.getenv("AIRFLOW_USERNAME", "admin"),
        password=os.getenv("AIRFLOW_PASSWORD", "admin"),
    )
except Exception:
    pass


class ConfigureMCPAdapterPlugin(AirflowPlugin):
    name = "configure_mcp_adapter"
