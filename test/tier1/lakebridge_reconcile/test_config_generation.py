"""Tier 1 tests for Lakebridge Reconcile config generation.

Tests 1-6: Validates that the agent generates correct reconcile.yml and
recon_config.yml when given various migration scenarios.
"""

import pytest

from test.tier1.lakebridge_reconcile.config_validator import (
    extract_yaml_blocks,
    validate_reconcile_yml,
    validate_recon_config_yml,
)


def _find_reconcile_yml(yaml_blocks: list[str]) -> str | None:
    """Find the reconcile.yml block (contains data_source and report_type)."""
    for block in yaml_blocks:
        if "data_source" in block and "report_type" in block:
            return block
    return None


def _find_recon_config_yml(yaml_blocks: list[str]) -> str | None:
    """Find the recon_config.yml block (contains tables list)."""
    for block in yaml_blocks:
        if "tables:" in block and ("source_name" in block or "target_name" in block):
            return block
    return None


@pytest.mark.tier1
class TestBasicConfig:
    """Test 1: Basic Snowflake reconciliation config generation."""

    def test_basic_snowflake_config(self, run_agent):
        """Agent should generate valid reconcile.yml and recon_config.yml for Snowflake."""
        result = run_agent(
            user_prompt=(
                "I migrated tables from Snowflake (PROD_DB.PUBLIC) to Databricks "
                "(main.migrated). Generate reconcile YAML configs to validate the "
                "migration. Tables: customers, orders, order_items, products. "
                "Use report_type 'all' for complete validation. "
                "Provide the complete YAML configs. Do not ask questions."
            ),
        )

        response = result["final_response"]
        yaml_blocks = extract_yaml_blocks(response)
        assert len(yaml_blocks) >= 2, (
            f"Expected at least 2 YAML blocks (reconcile.yml + recon_config.yml). "
            f"Got {len(yaml_blocks)}. Response:\n{response[:1000]}"
        )

        # Validate reconcile.yml
        reconcile_yml = _find_reconcile_yml(yaml_blocks)
        assert reconcile_yml is not None, (
            f"Could not find reconcile.yml block with data_source. "
            f"YAML blocks found: {[b[:100] for b in yaml_blocks]}"
        )
        errors = validate_reconcile_yml(reconcile_yml)
        assert errors == [], f"reconcile.yml validation errors: {errors}"

        # Check specific values
        assert "snowflake" in reconcile_yml.lower(), "data_source should be snowflake"
        assert "lakebridge_snowflake" in reconcile_yml, "secret_scope should be lakebridge_snowflake"

        # Validate recon_config.yml
        recon_config = _find_recon_config_yml(yaml_blocks)
        assert recon_config is not None, (
            f"Could not find recon_config.yml block with tables. "
            f"YAML blocks found: {[b[:100] for b in yaml_blocks]}"
        )
        errors = validate_recon_config_yml(recon_config, report_type="all")
        assert errors == [], f"recon_config.yml validation errors: {errors}"

        # Should have 4 tables
        assert recon_config.count("source_name") >= 4, (
            f"Expected 4 table definitions. Config:\n{recon_config[:500]}"
        )


@pytest.mark.tier1
class TestColumnMapping:
    """Test 2: Column mapping detection."""

    def test_column_mapping_detection(self, run_agent):
        """Agent should generate column_mapping when source/target column names differ."""
        result = run_agent(
            user_prompt=(
                "I migrated customers from Snowflake (PROD_DB.PUBLIC) to Databricks "
                "(main.migrated). The source table has columns 'cust_name' and "
                "'cust_email', but the target uses 'customer_name' and 'email'. "
                "Generate the recon_config.yml with appropriate column_mapping. "
                "Use data_source snowflake, report_type data, join on customer_id. "
                "Provide the complete YAML. Do not ask questions."
            ),
        )

        response = result["final_response"]
        yaml_blocks = extract_yaml_blocks(response)

        recon_config = _find_recon_config_yml(yaml_blocks)
        assert recon_config is not None, "Could not find recon_config.yml block"

        # Should contain column_mapping
        assert "column_mapping" in recon_config, (
            f"Expected column_mapping in config. Config:\n{recon_config[:500]}"
        )

        # Should map cust_name -> customer_name
        assert "cust_name" in recon_config, "Should reference source column cust_name"
        assert "customer_name" in recon_config, "Should reference target column customer_name"

        # Should map cust_email -> email
        assert "cust_email" in recon_config, "Should reference source column cust_email"

        # Should have join_columns
        assert "join_columns" in recon_config, "Should include join_columns"
        assert "customer_id" in recon_config, "join_columns should include customer_id"


@pytest.mark.tier1
class TestThresholds:
    """Test 3: Threshold configuration for decimal columns."""

    def test_threshold_config(self, run_agent):
        """Agent should generate column_thresholds with appropriate bounds."""
        result = run_agent(
            user_prompt=(
                "I'm reconciling order_items from Snowflake (PROD_DB.PUBLIC) to "
                "Databricks (main.migrated). The unit_price column may have small "
                "precision differences after migration (source has 4 decimal places, "
                "target has 2). Configure thresholds to allow up to 0.01 difference. "
                "Use data_source snowflake, report_type data, join on order_id + item_id. "
                "Also add transformations for NULL handling on unit_price. "
                "Provide the complete YAML. Do not ask questions."
            ),
        )

        response = result["final_response"]
        yaml_blocks = extract_yaml_blocks(response)

        recon_config = _find_recon_config_yml(yaml_blocks)
        assert recon_config is not None, "Could not find recon_config.yml block"

        # Should contain column_thresholds
        assert "column_thresholds" in recon_config, (
            f"Expected column_thresholds in config. Config:\n{recon_config[:500]}"
        )
        assert "unit_price" in recon_config, "Thresholds should reference unit_price"

        # Should contain threshold bounds
        config_lower = recon_config.lower()
        assert "0.01" in recon_config or "1%" in recon_config, (
            "Should have threshold bounds of 0.01 or 1%"
        )

        # Should contain transformations for NULL handling
        assert "transformation" in config_lower, (
            "Should include transformations for NULL handling"
        )
        assert "coalesce" in config_lower or "null_recon" in config_lower, (
            "Transformations should handle NULLs with coalesce or _null_recon_"
        )


@pytest.mark.tier1
class TestSchemaOnly:
    """Test 4: Schema-only report type selection."""

    def test_schema_only_config(self, run_agent):
        """Agent should generate schema-only config without requiring join_columns."""
        result = run_agent(
            user_prompt=(
                "I just want a quick schema check before running full data comparison. "
                "Source is Oracle (HR schema), target is main.hr_migrated. "
                "Tables: employees, departments. "
                "Generate reconcile.yml and recon_config.yml for schema-only check. "
                "Provide the complete YAML configs. Do not ask questions."
            ),
        )

        response = result["final_response"]
        yaml_blocks = extract_yaml_blocks(response)

        reconcile_yml = _find_reconcile_yml(yaml_blocks)
        assert reconcile_yml is not None, "Could not find reconcile.yml block"
        assert "schema" in reconcile_yml.lower(), "report_type should be schema"

        # Validate - schema type should not require join_columns
        recon_config = _find_recon_config_yml(yaml_blocks)
        assert recon_config is not None, "Could not find recon_config.yml block"
        errors = validate_recon_config_yml(recon_config, report_type="schema")
        assert errors == [], f"recon_config.yml validation errors: {errors}"

        # Should reference oracle
        assert "oracle" in reconcile_yml.lower(), "data_source should be oracle"


@pytest.mark.tier1
class TestFiltered:
    """Test 5: Filtered reconciliation with dialect-specific SQL."""

    def test_filtered_mssql_config(self, run_agent):
        """Agent should generate dialect-appropriate filters for source and target."""
        result = run_agent(
            user_prompt=(
                "Only reconcile orders from the last 90 days. "
                "Source is SQL Server (dbo schema), target is main.migrated. "
                "Generate reconcile.yml and recon_config.yml. "
                "Use report_type data, join on order_id. "
                "The filter should use the order_date column. "
                "Make sure to use T-SQL syntax for source filter and Databricks SQL "
                "for target filter. "
                "Provide the complete YAML configs. Do not ask questions."
            ),
        )

        response = result["final_response"]
        yaml_blocks = extract_yaml_blocks(response)

        reconcile_yml = _find_reconcile_yml(yaml_blocks)
        assert reconcile_yml is not None, "Could not find reconcile.yml block"
        assert "mssql" in reconcile_yml.lower(), "data_source should be mssql"
        assert "lakebridge_mssql" in reconcile_yml, "secret_scope should be lakebridge_mssql"

        recon_config = _find_recon_config_yml(yaml_blocks)
        assert recon_config is not None, "Could not find recon_config.yml block"

        # Should have filters section
        assert "filters" in recon_config, "Should include filters"

        config_lower = recon_config.lower()
        # Source filter should use T-SQL syntax
        has_tsql_filter = (
            "dateadd" in config_lower
            or "getdate" in config_lower
            or "datediff" in config_lower
        )
        assert has_tsql_filter, (
            f"Source filter should use T-SQL syntax (DATEADD/GETDATE). "
            f"Config:\n{recon_config[:500]}"
        )

        # Target filter should use Databricks SQL syntax
        has_databricks_filter = (
            "date_sub" in config_lower
            or "current_date" in config_lower
            or "dateadd" in config_lower  # also valid in Databricks SQL
        )
        assert has_databricks_filter, (
            f"Target filter should use Databricks SQL syntax. "
            f"Config:\n{recon_config[:500]}"
        )


@pytest.mark.tier1
class TestAggregates:
    """Test 6: Aggregate validation config."""

    def test_aggregate_config(self, run_agent):
        """Agent should generate aggregates with correct types and grouping."""
        result = run_agent(
            user_prompt=(
                "Generate recon_config.yml to verify aggregate metrics match between "
                "source and target for the orders table. Check: "
                "SUM(total_amount) grouped by region, "
                "COUNT(order_id) overall, and "
                "AVG(discount) grouped by status. "
                "Source is Snowflake (PROD_DB.PUBLIC), target is main.migrated. "
                "Join on order_id. "
                "Provide the complete YAML. Do not ask questions."
            ),
        )

        response = result["final_response"]
        yaml_blocks = extract_yaml_blocks(response)

        recon_config = _find_recon_config_yml(yaml_blocks)
        assert recon_config is not None, "Could not find recon_config.yml block"

        # Should have aggregates section
        assert "aggregates" in recon_config, "Should include aggregates section"

        config_lower = recon_config.lower()

        # Should have SUM aggregate
        assert "sum" in config_lower, "Should include SUM aggregate"
        assert "total_amount" in config_lower, "SUM should reference total_amount"

        # Should have COUNT aggregate
        assert "count" in config_lower, "Should include COUNT aggregate"
        assert "order_id" in config_lower, "COUNT should reference order_id"

        # Should have AVG aggregate
        assert "avg" in config_lower, "Should include AVG aggregate"

        # Should have group_by_columns
        assert "group_by" in config_lower, "Should include group_by_columns"

        # Validate the config
        errors = validate_recon_config_yml(recon_config, report_type="data")
        assert errors == [], f"recon_config.yml validation errors: {errors}"
