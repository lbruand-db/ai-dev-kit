"""Mock Databricks tool definitions and handlers for Tier 1 Lakebridge Reconcile tests.

Provides OpenAI function-calling format tool definitions and Python handlers.
SQL queries are executed against a DuckDB in-memory database via SQLGlot
transpilation. Non-SQL tools return pre-canned fixture data.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from test.tier1.lakebridge_reconcile.duckdb_backend import execute_sql
from test.tier1.lakebridge_reconcile.fixtures.databricks_responses import (
    FIXTURES,
    SECRET_SCOPES,
)

if TYPE_CHECKING:
    import duckdb

# ---------------------------------------------------------------------------
# Tool definitions (OpenAI function-calling format)
# ---------------------------------------------------------------------------

TOOL_DEFINITIONS = [
    {
        "type": "function",
        "function": {
            "name": "connect_to_workspace",
            "description": "Connect to a Databricks workspace by name.",
            "parameters": {
                "type": "object",
                "properties": {
                    "workspace": {
                        "type": "string",
                        "description": "Name or URL of the workspace to connect to",
                    }
                },
                "required": ["workspace"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "execute_sql",
            "description": (
                "Execute a SQL statement on Databricks and return results as CSV. "
                "Supports Databricks SQL dialect including SHOW TABLES, DESCRIBE TABLE, "
                "SELECT, and other standard SQL statements."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "statement": {
                        "type": "string",
                        "description": "The SQL statement to execute",
                    },
                    "parameters": {
                        "type": "array",
                        "items": {"type": "object"},
                        "description": "Optional query parameters",
                    },
                },
                "required": ["statement"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_warehouses",
            "description": "List SQL warehouses available in the Databricks workspace.",
            "parameters": {
                "type": "object",
                "properties": {},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_secrets",
            "description": "List secret keys in a Databricks secret scope. Returns key names only (values are hidden).",
            "parameters": {
                "type": "object",
                "properties": {
                    "scope": {
                        "type": "string",
                        "description": "Name of the secret scope",
                    }
                },
                "required": ["scope"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_secret",
            "description": "Get a secret value from a Databricks secret scope. Returns REDACTED for security.",
            "parameters": {
                "type": "object",
                "properties": {
                    "scope": {
                        "type": "string",
                        "description": "Name of the secret scope",
                    },
                    "key": {
                        "type": "string",
                        "description": "Name of the secret key",
                    },
                },
                "required": ["scope", "key"],
            },
        },
    },
]


# ---------------------------------------------------------------------------
# Tool handler factory
# ---------------------------------------------------------------------------


def create_tool_handlers(conn: duckdb.DuckDBPyConnection) -> dict[str, callable]:
    """Create tool handlers bound to a DuckDB connection.

    SQL queries are transpiled via SQLGlot and executed on DuckDB.
    Non-SQL tools return pre-canned fixtures.
    """

    def handle_connect_to_workspace(workspace: str = "", **kwargs: object) -> str:
        return FIXTURES["workspace_connected"]

    def handle_execute_sql(
        statement: str, parameters: list | None = None, **kwargs: object
    ) -> str:
        return execute_sql(conn, statement)

    def handle_list_warehouses(**kwargs: object) -> str:
        return FIXTURES["warehouses"]

    def handle_list_secrets(scope: str = "", **kwargs: object) -> str:
        keys = SECRET_SCOPES.get(scope)
        if keys is None:
            return json.dumps({"error": f"Secret scope '{scope}' not found"})
        return json.dumps({"keys": [{"key": k} for k in keys]})

    def handle_get_secret(scope: str = "", key: str = "", **kwargs: object) -> str:
        keys = SECRET_SCOPES.get(scope)
        if keys is None:
            return json.dumps({"error": f"Secret scope '{scope}' not found"})
        if key not in keys:
            return json.dumps({"error": f"Key '{key}' not found in scope '{scope}'"})
        return json.dumps({"value": "<REDACTED>"})

    return {
        "connect_to_workspace": handle_connect_to_workspace,
        "execute_sql": handle_execute_sql,
        "list_warehouses": handle_list_warehouses,
        "list_secrets": handle_list_secrets,
        "get_secret": handle_get_secret,
    }
