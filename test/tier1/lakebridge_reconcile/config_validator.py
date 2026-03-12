"""Validates generated YAML configs against Lakebridge reconciliation schema.

Parses YAML output from the agent and validates field values, types,
and constraints without requiring the actual Lakebridge library.
"""

from __future__ import annotations

import ast
import re

import yaml

# ---------------------------------------------------------------------------
# Valid enum values
# ---------------------------------------------------------------------------

VALID_DATA_SOURCES = {"snowflake", "oracle", "mssql", "synapse", "databricks"}
VALID_REPORT_TYPES = {"schema", "row", "data", "all"}
VALID_AGGREGATE_TYPES = {
    "min", "max", "count", "sum", "avg", "mean",
    "mode", "stddev", "variance", "median",
}
VALID_THRESHOLD_TYPES = {
    "int", "integer", "bigint", "smallint", "tinyint",
    "float", "double", "decimal", "numeric",
    "date", "timestamp",
}
DEFAULT_SECRET_SCOPES = {
    "snowflake": "lakebridge_snowflake",
    "oracle": "lakebridge_oracle",
    "mssql": "lakebridge_mssql",
    "synapse": "lakebridge_synapse",
    "databricks": "",
}


# ---------------------------------------------------------------------------
# YAML extraction helpers
# ---------------------------------------------------------------------------


def extract_yaml_blocks(text: str) -> list[str]:
    """Extract YAML code blocks from agent response text."""
    blocks = re.findall(r"```ya?ml\s*\n(.*?)```", text, re.DOTALL)
    return blocks


def extract_python_blocks(text: str) -> list[str]:
    """Extract Python code blocks from agent response text."""
    blocks = re.findall(r"```python\s*\n(.*?)```", text, re.DOTALL)
    return blocks


# ---------------------------------------------------------------------------
# reconcile.yml validation
# ---------------------------------------------------------------------------


def validate_reconcile_yml(yaml_str: str) -> list[str]:
    """Parse and validate reconcile.yml content. Returns list of errors (empty = valid)."""
    errors = []

    try:
        data = yaml.safe_load(yaml_str)
    except yaml.YAMLError as e:
        return [f"YAML parse error: {e}"]

    if not isinstance(data, dict):
        return ["reconcile.yml must be a YAML mapping"]

    # version
    version = data.get("version")
    if version != 1:
        errors.append(f"version must be 1, got {version}")

    # data_source
    ds = data.get("data_source", "")
    if ds not in VALID_DATA_SOURCES:
        errors.append(f"data_source must be one of {VALID_DATA_SOURCES}, got '{ds}'")

    # report_type
    rt = data.get("report_type", "")
    if rt not in VALID_REPORT_TYPES:
        errors.append(f"report_type must be one of {VALID_REPORT_TYPES}, got '{rt}'")

    # secret_scope
    ss = data.get("secret_scope")
    if ds != "databricks" and not ss:
        errors.append("secret_scope is required when data_source is not 'databricks'")

    # database_config
    db_config = data.get("database_config", {})
    if not isinstance(db_config, dict):
        errors.append("database_config must be a mapping")
    else:
        if not db_config.get("source_schema"):
            errors.append("database_config.source_schema is required")
        if not db_config.get("target_catalog"):
            errors.append("database_config.target_catalog is required")
        if not db_config.get("target_schema"):
            errors.append("database_config.target_schema is required")

    return errors


# ---------------------------------------------------------------------------
# recon_config.yml validation
# ---------------------------------------------------------------------------


def validate_recon_config_yml(yaml_str: str, report_type: str = "all") -> list[str]:
    """Parse and validate recon_config.yml content. Returns list of errors (empty = valid)."""
    errors = []

    try:
        data = yaml.safe_load(yaml_str)
    except yaml.YAMLError as e:
        return [f"YAML parse error: {e}"]

    if not isinstance(data, dict):
        return ["recon_config.yml must be a YAML mapping"]

    # version
    version = data.get("version")
    if version != 2:
        errors.append(f"version must be 2, got {version}")

    # tables
    tables = data.get("tables", [])
    if not isinstance(tables, list) or len(tables) == 0:
        errors.append("tables must be a non-empty list")
        return errors

    for i, table in enumerate(tables):
        prefix = f"tables[{i}]"

        if not isinstance(table, dict):
            errors.append(f"{prefix} must be a mapping")
            continue

        # source_name / target_name
        if not table.get("source_name"):
            errors.append(f"{prefix}.source_name is required")
        if not table.get("target_name"):
            errors.append(f"{prefix}.target_name is required")

        # join_columns required for data/all
        if report_type in ("data", "all"):
            jc = table.get("join_columns")
            if not jc or not isinstance(jc, list) or len(jc) == 0:
                errors.append(
                    f"{prefix}.join_columns is required for report_type '{report_type}'"
                )

        # column_mapping
        cm = table.get("column_mapping", [])
        if cm:
            if not isinstance(cm, list):
                errors.append(f"{prefix}.column_mapping must be a list")
            else:
                for j, mapping in enumerate(cm):
                    if not isinstance(mapping, dict):
                        errors.append(f"{prefix}.column_mapping[{j}] must be a mapping")
                    elif not mapping.get("source_name") or not mapping.get("target_name"):
                        errors.append(
                            f"{prefix}.column_mapping[{j}] requires source_name and target_name"
                        )

        # column_thresholds
        ct = table.get("column_thresholds", [])
        if ct:
            if not isinstance(ct, list):
                errors.append(f"{prefix}.column_thresholds must be a list")
            else:
                for j, thresh in enumerate(ct):
                    if not isinstance(thresh, dict):
                        errors.append(f"{prefix}.column_thresholds[{j}] must be a mapping")
                        continue
                    if not thresh.get("column_name"):
                        errors.append(f"{prefix}.column_thresholds[{j}].column_name required")
                    t_type = str(thresh.get("type", "")).lower()
                    if t_type and t_type not in VALID_THRESHOLD_TYPES:
                        errors.append(
                            f"{prefix}.column_thresholds[{j}].type '{t_type}' "
                            f"not in {VALID_THRESHOLD_TYPES}"
                        )

        # table_thresholds
        tt = table.get("table_thresholds", [])
        if tt:
            if not isinstance(tt, list):
                errors.append(f"{prefix}.table_thresholds must be a list")
            else:
                for j, thresh in enumerate(tt):
                    if not isinstance(thresh, dict):
                        errors.append(f"{prefix}.table_thresholds[{j}] must be a mapping")
                        continue
                    model = thresh.get("model", "")
                    if model and model != "mismatch":
                        errors.append(
                            f"{prefix}.table_thresholds[{j}].model must be 'mismatch', got '{model}'"
                        )

        # aggregates
        aggs = table.get("aggregates", [])
        if aggs:
            if not isinstance(aggs, list):
                errors.append(f"{prefix}.aggregates must be a list")
            else:
                for j, agg in enumerate(aggs):
                    if not isinstance(agg, dict):
                        errors.append(f"{prefix}.aggregates[{j}] must be a mapping")
                        continue
                    agg_type = str(agg.get("type", "")).lower()
                    if agg_type not in VALID_AGGREGATE_TYPES:
                        errors.append(
                            f"{prefix}.aggregates[{j}].type '{agg_type}' "
                            f"not in {VALID_AGGREGATE_TYPES}"
                        )
                    if not agg.get("agg_columns"):
                        errors.append(
                            f"{prefix}.aggregates[{j}].agg_columns is required"
                        )

        # jdbc_reader_options
        jdbc = table.get("jdbc_reader_options")
        if jdbc:
            if not isinstance(jdbc, dict):
                errors.append(f"{prefix}.jdbc_reader_options must be a mapping")
            else:
                for field in ("number_partitions", "partition_column", "lower_bound", "upper_bound"):
                    if field not in jdbc:
                        errors.append(f"{prefix}.jdbc_reader_options.{field} is required")

    return errors


# ---------------------------------------------------------------------------
# Notebook code validation
# ---------------------------------------------------------------------------


def validate_notebook_code(code_str: str) -> list[str]:
    """Parse and validate generated notebook code. Returns list of errors (empty = valid)."""
    errors = []

    # Strip notebook magic commands for syntax check
    clean_lines = []
    for line in code_str.split("\n"):
        stripped = line.strip()
        if stripped.startswith("%") or stripped.startswith("dbutils.library.restartPython"):
            continue
        clean_lines.append(line)
    clean_code = "\n".join(clean_lines)

    # Check syntax validity
    try:
        ast.parse(clean_code)
    except SyntaxError as e:
        errors.append(f"Python syntax error: {e}")

    code_lower = code_str.lower()

    # Check imports from correct modules
    if "databricks.labs.lakebridge.config" not in code_str:
        errors.append("Missing import from databricks.labs.lakebridge.config")

    # Check key classes are used
    if "ReconcileConfig" not in code_str:
        errors.append("Missing ReconcileConfig usage")
    if "TableRecon" not in code_str and "Table(" not in code_str:
        errors.append("Missing TableRecon or Table usage")

    # Check trigger service is called
    if "trigger_recon" not in code_lower:
        errors.append("Missing TriggerReconService.trigger_recon() call")

    return errors
