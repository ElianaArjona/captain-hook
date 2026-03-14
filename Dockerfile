FROM apache/airflow:3.0.2

USER airflow

# Install agent deps, then pin starlette back to what Airflow's FastAPI requires.
# astro-airflow-mcp -> fastmcp -> starlette 0.52.x, which breaks Airflow's
# internal Task Execution API (needs starlette<0.47.0).
RUN pip install --no-cache-dir \
    "anthropic>=0.40.0" \
    "mcp>=1.0.0" \
    "astro-airflow-mcp" && \
    pip install --no-cache-dir --force-reinstall "starlette>=0.40.0,<0.47.0"
