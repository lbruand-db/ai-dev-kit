"""Tier 1 tests for Lakebridge Reconcile notebook code generation.

Tests 7-9: Validates that the agent generates correct Python notebook code
with proper imports, class usage, and configuration patterns.
"""

import pytest

from test.tier1.lakebridge_reconcile.config_validator import (
    extract_python_blocks,
    validate_notebook_code,
)


def _combine_python_blocks(python_blocks: list[str]) -> str:
    """Combine all Python blocks into a single string for content assertions.

    The agent often splits notebook code across multiple cells. We combine
    them so assertions can find content that spans cells.
    """
    relevant = []
    for block in python_blocks:
        stripped = block.strip()
        # Skip standalone pip install or dbutils restart lines
        if stripped.startswith("%pip") and "\n" not in stripped:
            continue
        if stripped.startswith("dbutils.") and "\n" not in stripped:
            continue
        relevant.append(block)
    return "\n\n".join(relevant)


@pytest.mark.tier1
class TestSnowflakeNotebook:
    """Test 7: Valid notebook code for Snowflake source."""

    def test_valid_snowflake_notebook(self, run_agent):
        """Agent should generate syntactically valid Python with correct imports."""
        result = run_agent(
            user_prompt=(
                "Generate a Python notebook that runs Lakebridge reconciliation "
                "for Snowflake source (PROD_DB.PUBLIC) to Databricks target "
                "(main.migrated). Tables: customers, orders. Join on customer_id "
                "and order_id respectively. Use report_type 'all'. "
                "Provide the complete Python notebook code. Do not ask questions."
            ),
        )

        response = result["final_response"]
        python_blocks = extract_python_blocks(response)
        assert len(python_blocks) >= 1, (
            f"Expected at least 1 Python code block. "
            f"Got {len(python_blocks)}. Response:\n{response[:1000]}"
        )

        # Combine all blocks for content assertions
        all_code = _combine_python_blocks(python_blocks)

        # Validate syntax on combined code
        errors = validate_notebook_code(all_code)
        assert errors == [], f"Notebook validation errors: {errors}"

        # Check specific imports
        assert "databricks.labs.lakebridge.config" in all_code, (
            "Should import from databricks.labs.lakebridge.config"
        )
        assert "ReconcileConfig" in all_code, "Should use ReconcileConfig"
        assert "DatabaseConfig" in all_code, "Should use DatabaseConfig"

        # Check data source and scope (across all cells)
        assert "snowflake" in all_code.lower(), "Should specify snowflake data_source"
        assert "lakebridge_snowflake" in all_code, "Should use lakebridge_snowflake scope"

        # Check trigger
        assert "trigger_recon" in all_code, "Should call trigger_recon"


@pytest.mark.tier1
class TestJdbcNotebook:
    """Test 8: Notebook with JDBC reader options for large tables."""

    def test_jdbc_reader_options_notebook(self, run_agent):
        """Agent should generate notebook with JdbcReaderOptions for large Oracle table."""
        result = run_agent(
            user_prompt=(
                "Generate Python notebook code for reconciling a 100M-row orders "
                "table from Oracle to Databricks. Source: Oracle (APP schema), "
                "target: main.migrated. Include JDBC reader options for parallel "
                "reads — partition on order_id with appropriate bounds. "
                "Use report_type 'data', join on order_id. "
                "Provide the complete Python notebook code. Do not ask questions."
            ),
        )

        response = result["final_response"]
        python_blocks = extract_python_blocks(response)
        assert len(python_blocks) >= 1, (
            f"Expected at least 1 Python code block. Response:\n{response[:1000]}"
        )

        all_code = _combine_python_blocks(python_blocks)

        # Should import JdbcReaderOptions
        assert "JdbcReaderOptions" in all_code, (
            f"Should use JdbcReaderOptions. Code:\n{all_code[:500]}"
        )

        # Should have partition configuration
        assert "number_partitions" in all_code, "Should set number_partitions"
        assert "partition_column" in all_code, "Should set partition_column"

        # Should reference oracle (across all cells)
        assert "oracle" in all_code.lower(), "Should specify oracle data_source"
        all_code_lower = all_code.lower()
        assert (
            "lakebridge_oracle" in all_code
            or "lakebridge_oracle" in all_code_lower
        ), "Should use lakebridge_oracle scope"

        # Should have valid syntax
        errors = validate_notebook_code(all_code)
        assert errors == [], f"Notebook validation errors: {errors}"


@pytest.mark.tier1
class TestDatabricksNotebook:
    """Test 9: Databricks-to-Databricks notebook."""

    def test_databricks_to_databricks_notebook(self, run_agent):
        """Agent should generate notebook with empty secret_scope for Databricks source."""
        result = run_agent(
            user_prompt=(
                "Generate a Python notebook to validate Hive metastore migration "
                "to Unity Catalog. Source: hive_metastore.legacy_db, target: "
                "main.migrated_db. Tables: users, products, orders (all join on id). "
                "Use report_type 'all'. data_source is 'databricks'. "
                "No secret scope is needed since both sides are Databricks. "
                "Provide the complete Python notebook code. Do not ask questions."
            ),
        )

        response = result["final_response"]
        python_blocks = extract_python_blocks(response)
        assert len(python_blocks) >= 1, (
            f"Expected at least 1 Python code block. Response:\n{response[:1000]}"
        )

        all_code = _combine_python_blocks(python_blocks)

        # Should specify databricks data_source
        all_code_lower = all_code.lower()
        assert '"databricks"' in all_code_lower or "'databricks'" in all_code_lower, (
            f"Should specify data_source='databricks'. Code:\n{all_code[:500]}"
        )

        # secret_scope should be empty string
        assert '""' in all_code or "''" in all_code, (
            f"secret_scope should be empty string for Databricks-to-Databricks. "
            f"Code:\n{all_code[:500]}"
        )

        # Should reference hive_metastore as source catalog
        assert "hive_metastore" in all_code, (
            "Should use hive_metastore as source_catalog"
        )

        # Should have valid syntax
        errors = validate_notebook_code(all_code)
        assert errors == [], f"Notebook validation errors: {errors}"
