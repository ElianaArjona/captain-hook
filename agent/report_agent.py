"""
DAG Failure Report Agent

Uses Claude + the Airflow MCP (astro-airflow-mcp) to analyze ALL recent
DAG failures and generate a consolidated markdown report.

Flow:
  1. Connect to astro-airflow-mcp via HTTP (at /mcp/v1/ on the api-server)
  2. Expose all MCP tools to Claude via the beta tool_runner
  3. Claude autonomously calls get_system_health, list_dag_runs,
     diagnose_dag_run, get_task_logs, list_assets, etc.
  4. Claude writes the final markdown report in its last message
  5. Report is saved to the REPORTS_DIR directory
"""

import asyncio
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import anthropic
import httpx
from anthropic.lib.tools.mcp import async_mcp_tool
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

AIRFLOW_API_URL = os.getenv("AIRFLOW_API_URL", "http://localhost:8080")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
REPORTS_DIR = Path(os.getenv("REPORTS_DIR", "/opt/airflow/reports"))
LOOKBACK_HOURS = int(os.getenv("REPORT_LOOKBACK_HOURS", "1"))

# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = f"""You are an Airflow on-call analyst. Investigate ALL DAG failures from the
last {LOOKBACK_HOURS} hour(s) using the available MCP tools and write a short incident report.

Steps:
1. get_system_health() — overview
2. For each failed DAG: diagnose_dag_run() to get the error
3. Note any patterns (same error, same owner, cascading)

Be concise. Engineers need to scan this in under 2 minutes.
"""

USER_PROMPT = f"""Write a DAG failure report for the last {LOOKBACK_HOURS} hour(s). Triggered by: <triggered_by_value>. Generated: <timestamp>.

Use this format (keep each section SHORT):

# DAG Failure Report — <timestamp>
**Triggered by:** <triggered_by_value> | **Window:** last {LOOKBACK_HOURS}h | **Failed DAGs:** N

---

## Summary
One sentence describing the situation.

---

## Failed DAGs

**`<dag_id>`** — 🔴/🟠/🟡 <Severity>
- **Failed task:** `<task_id>` at <time>
- **Error:** `<one-line error>`
- **Fix:** <1-2 sentence suggested action>

(repeat for each failed DAG, no tables, no long blocks)

---

## Action Items
1. Most urgent fix
2. Second fix

---

If NO failures in the last {LOOKBACK_HOURS}h, write: "✅ No DAG failures in the last {LOOKBACK_HOURS}h." plus a one-line health summary.
"""


# ---------------------------------------------------------------------------
# Agent
# ---------------------------------------------------------------------------


async def _get_airflow_token() -> str:
    """Obtain a Bearer token from Airflow's auth endpoint."""
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{AIRFLOW_API_URL}/auth/token",
            json={"username": AIRFLOW_USERNAME, "password": AIRFLOW_PASSWORD},
        )
        resp.raise_for_status()
        return resp.json()["access_token"]


async def _generate_report(triggered_by: str) -> str:
    """Connect to MCP over HTTP, run the Claude agent, return the markdown report."""

    # Get JWT token for authenticating against the Airflow API (used by MCP tools)
    token = await _get_airflow_token()

    # MCP URL found at /mcp/v1/ here https://github.com/astronomer/agents/blob/main/astro-airflow-mcp/src/astro_airflow_mcp/plugin.py#L40-L44
    mcp_url = f"{AIRFLOW_API_URL}/mcp/v1/"

    async with streamablehttp_client(
        mcp_url,
        headers={"Authorization": f"Bearer {token}"},
    ) as (read, write, _):
        async with ClientSession(read, write) as mcp_session:
            await mcp_session.initialize()

            tools_result = await mcp_session.list_tools()
            mcp_tools = [
                async_mcp_tool(t, mcp_session) for t in tools_result.tools
            ]

            client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

            # Inject the trigger context and timestamp into the user prompt
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            prompt = USER_PROMPT.replace(
                "<triggered_by_value>", triggered_by
            ).replace("<timestamp>", timestamp)

            messages = [{"role": "user", "content": prompt}]
            final_report = ""

            # tool_runner handles the agentic loop automatically
            runner = client.beta.messages.tool_runner(
                model="claude-opus-4-6",
                max_tokens=8096,
                system=SYSTEM_PROMPT,
                thinking={"type": "adaptive"},
                tools=mcp_tools,
                messages=messages,
            )

            async for message in runner:
                # Collect text from every assistant turn; the last one is the report
                for block in message.content:
                    if hasattr(block, "text") and block.type == "text":
                        final_report = block.text  # keep overwriting → last text wins

            return final_report or "Error: agent produced no text output."


def _save_report(content: str, triggered_by: str) -> str:
    """Write the markdown report to disk, return the file path."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M")
    safe_trigger = triggered_by.replace("/", "_").replace(" ", "_")[:40]
    filename = f"{timestamp}_{safe_trigger}.md"
    filepath = REPORTS_DIR / filename

    filepath.write_text(content, encoding="utf-8")
    print(f"[captain-hook] Report saved: {filepath}", file=sys.stderr)
    return str(filepath)


def run_report(triggered_by: str = "scheduled") -> str:
    """Entry point called by the Airflow DAG task. Returns the report file path."""
    report_content = asyncio.run(_generate_report(triggered_by))
    return _save_report(report_content, triggered_by)


# ---------------------------------------------------------------------------
# CLI usage (for local testing outside Airflow)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    _triggered_by = sys.argv[1] if len(sys.argv) > 1 else "manual"
    path = run_report(triggered_by=_triggered_by)
    print(f"Report written to: {path}")
