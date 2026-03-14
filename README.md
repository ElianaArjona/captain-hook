# captain-hook
**MCP Airflow DAG Failure Report**

Consolidates noisy per-DAG alerts into a single, actionable markdown incident report using Claude + the [Airflow MCP server](https://github.com/astronomer/agents/blob/main/astro-airflow-mcp/README.md).

---

## The Problem

20 DAGs fail → 20 individual alerts → developers drown in noise.

## The Solution

One report, every 30 minutes (or on the first failure), covering every failed DAG with root cause, suggested fix, and downstream impact.

---

## Architecture

```
                    ┌─────────────────────────────────────┐
 Any DAG fails ──►  │  on_failure_callback (callbacks.py) │
                    │  POST /api/v2/dags/dag_failure_report│
                    └────────────────┬────────────────────┘
                                     │ triggers (max_active_runs=1)
                    ┌────────────────▼────────────────────┐
 Every 30 min  ──►  │     dag_failure_report (Airflow)     │
                    │     dag_report_generator.py          │
                    └────────────────┬────────────────────┘
                                     │ calls
                    ┌────────────────▼────────────────────┐
                    │       agent/report_agent.py          │
                    │  Claude Opus 4.6 + Airflow MCP tools │
                    └────────────────┬────────────────────┘
                                     │ writes
                    ┌────────────────▼────────────────────┐
                    │  reports/YYYY-MM-DD_HH-MM_<name>.md  │
                    └─────────────────────────────────────┘
```

**Key design decisions:**
- `max_active_runs=1` on the report DAG prevents duplicate reports when many DAGs fail simultaneously
- The agent always queries **all** failures in the last hour, not just the one that triggered it
- `AF_READ_ONLY=true` on the MCP server prevents accidental mutations

---

## Sample Failing DAGs

| DAG ID | Failure Mode |
|---|---|
| `failing_db_connection` | Can't connect to analytics DB (wrong host) |
| `failing_python_error` | `KeyError` — missing field in API payload |
| `failing_timeout` | ML training task exceeds `execution_timeout` |
| `failing_upstream_sensor` | `FileSensor` times out waiting for partner feed |
| `failing_data_quality` | Revenue / order count anomalies detected |

---

## Report Contents (per failed DAG)

- Name, schedule, owner
- Which task failed + full error message
- Failure timestamp
- Root cause analysis
- Suggested remediation steps (numbered)
- Downstream asset / data dependency impact
- Severity rating (🔴 Critical → 🟢 Low)

Plus a top-level summary with patterns and priority action items.

---

## Setup

### 1. Clone and configure

```bash
cp .env.example .env
# Edit .env and add your ANTHROPIC_API_KEY
```

### 2. Start Airflow 3

```bash
echo -e "AIRFLOW_UID=$(id -u)" >> .env
docker compose up -d
```

First startup takes ~3-5 minutes while pip installs `anthropic[mcp]` and `astro-airflow-mcp` in the containers.

### 3. Open Airflow UI

[http://localhost:8080](http://localhost:8080) — login: `admin` / `admin`

All 6 DAGs will appear (5 failing + 1 report generator). Unpause them all.

### 4. Trigger a failure

The failing DAGs run `@hourly`. To see a report immediately, manually trigger any failing DAG from the UI — it will fail, call the callback, and the report DAG will start.

### 5. Read the report

```bash
ls -lt reports/
cat reports/<latest>.md
```

---

## Project Structure

```
captain-hook/
├── docker-compose.yml
├── .env.example
├── dags/
│   ├── callbacks.py                    # Shared on_failure_callback
│   ├── dag_report_generator.py         # Report DAG (every 30 min)
│   └── failing_dags/
│       ├── dag_db_connection.py
│       ├── dag_python_error.py
│       ├── dag_timeout.py
│       ├── dag_upstream_sensor.py
│       └── dag_data_quality.py
├── agent/
│   ├── report_agent.py                 # Claude + MCP agent
│   └── requirements.txt
└── reports/                            # Generated markdown reports
```

---

## Local Agent Testing (without Airflow)

```bash
cd agent
pip install -r requirements.txt

export ANTHROPIC_API_KEY=sk-ant-...
export AIRFLOW_API_URL=http://localhost:8080
export AIRFLOW_USERNAME=admin
export AIRFLOW_PASSWORD=admin
export REPORTS_DIR=../reports

python report_agent.py manual
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `ANTHROPIC_API_KEY` | required | Claude API key |
| `AIRFLOW_API_URL` | `http://localhost:8080` | Airflow webserver URL |
| `AIRFLOW_USERNAME` | `admin` | Airflow basic auth username |
| `AIRFLOW_PASSWORD` | `admin` | Airflow basic auth password |
| `REPORTS_DIR` | `/opt/airflow/reports` | Where to save reports |
| `REPORT_LOOKBACK_HOURS` | `1` | How far back to look for failures |

---

## Future: Slack Integration

The `_save_report` function in `agent/report_agent.py` is the place to add Slack posting:

```python
def _save_report(content: str, triggered_by: str) -> str:
    filepath = ...  # existing logic
    # TODO: post to Slack
    # slack_client.chat_postMessage(channel="#alerts", text=content)
    return str(filepath)
```
