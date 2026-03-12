"""Pytest fixtures for Tier 1 Lakebridge Reconcile skill logic tests."""

import os
from pathlib import Path

import pytest

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from openai import OpenAI

from test.tier1.lakebridge_reconcile.agent_runner import run_skill_agent
from test.tier1.lakebridge_reconcile.duckdb_backend import create_test_database
from test.tier1.lakebridge_reconcile.mock_tools import TOOL_DEFINITIONS, create_tool_handlers


def _get_databricks_host() -> str:
    """Resolve Databricks host from env or ~/.databrickscfg DEFAULT profile."""
    host = os.environ.get("DATABRICKS_HOST", "")
    if host:
        return host
    # Fall back to ~/.databrickscfg default profile
    cfg_path = Path.home() / ".databrickscfg"
    if cfg_path.exists():
        import configparser

        cfg = configparser.ConfigParser()
        cfg.read(cfg_path)
        if cfg.has_option("DEFAULT", "host"):
            return cfg.get("DEFAULT", "host")
    return ""


@pytest.fixture(scope="session")
def client():
    """OpenAI client pointing at Databricks Foundation Model API."""
    host = _get_databricks_host()
    token = os.environ.get("DATABRICKS_TOKEN")
    if not token:
        pytest.skip("DATABRICKS_TOKEN not set")
    if not host:
        pytest.skip("DATABRICKS_HOST not set and not in ~/.databrickscfg")
    return OpenAI(api_key=token, base_url=f"{host.rstrip('/')}/serving-endpoints")


@pytest.fixture(scope="session")
def duckdb_conn():
    """In-memory DuckDB database seeded with Lakebridge reconcile test data."""
    conn = create_test_database()
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def skill_prompt():
    """Load the Lakebridge Reconcile skill files as a combined system prompt."""
    skill_dir = (
        Path(__file__).parents[3]
        / "databricks-skills"
        / "databricks-lakebridge-reconcile"
    )

    parts = []
    for filename in ("SKILL.md", "configuration.md", "examples.md", "secret_scopes.md"):
        filepath = skill_dir / filename
        if filepath.exists():
            parts.append(filepath.read_text())

    return "\n\n---\n\n".join(parts)


@pytest.fixture
def tools():
    """OpenAI-format tool definitions for mock Databricks tools."""
    return TOOL_DEFINITIONS


@pytest.fixture
def tool_handlers(duckdb_conn):
    """Tool handlers bound to the DuckDB test database."""
    return create_tool_handlers(duckdb_conn)


@pytest.fixture
def run_agent(client, tools, tool_handlers, skill_prompt):
    """Convenience fixture: returns a callable that runs the agent loop."""

    def _run(user_prompt: str, max_turns: int = 20) -> dict:
        return run_skill_agent(
            client=client,
            system_prompt=skill_prompt,
            user_prompt=user_prompt,
            tools=tools,
            tool_handlers=tool_handlers,
            max_turns=max_turns,
        )

    return _run
