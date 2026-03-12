"""Tier 1 tests for Lakebridge Reconcile result interpretation.

Tests 10-12: Validates that the agent correctly queries reconciliation
output tables and interprets results (pass/fail status, missing rows,
column mismatches).
"""

import pytest


@pytest.mark.tier1
class TestOverallStatus:
    """Test 10: Interpret overall reconciliation status."""

    def test_interpret_recon_status(self, run_agent):
        """Agent should query main_metrics and report per-table pass/fail status."""
        result = run_agent(
            user_prompt=(
                "My Lakebridge reconciliation ran with recon_id='abc123'. "
                "What's the overall status? Query the reconciliation output "
                "tables in remorph.reconcile to check. "
                "Proceed without asking questions."
            ),
        )

        # Agent should have queried main_metrics
        sql_calls = [tc for tc in result["tool_calls"] if tc["name"] == "execute_sql"]
        sql_texts = [tc["arguments"].get("statement", "") for tc in sql_calls]
        sql_upper = [s.upper() for s in sql_texts]

        assert any("MAIN_METRICS" in s for s in sql_upper), (
            f"Expected query on main_metrics table. SQL calls: {sql_texts}"
        )
        assert any("ABC123" in s for s in sql_upper), (
            f"Expected recon_id 'abc123' in query. SQL calls: {sql_texts}"
        )

        # Agent should report status in final response
        response = result["final_response"].lower()

        # Should mention tables with issues
        assert "customer" in response, (
            f"Should mention customers table (has issues). Response:\n{result['final_response'][:500]}"
        )
        assert "order_item" in response or "order item" in response, (
            f"Should mention order_items table (has issues). Response:\n{result['final_response'][:500]}"
        )

        # Should distinguish passing vs failing
        has_status_info = (
            ("pass" in response or "success" in response or "100" in response)
            and ("fail" in response or "issue" in response or "missing" in response or "mismatch" in response)
        )
        assert has_status_info, (
            f"Should report both passing and failing tables. Response:\n{result['final_response'][:500]}"
        )


@pytest.mark.tier1
class TestMissingRows:
    """Test 11: Diagnose missing rows."""

    def test_diagnose_missing_rows(self, run_agent):
        """Agent should query missing_in_tgt and identify specific missing customer IDs."""
        result = run_agent(
            user_prompt=(
                "My Lakebridge reconciliation (recon_id='abc123') shows 2 customers "
                "missing in target. Query the remorph.reconcile tables to find out "
                "which customers are missing and suggest how to fix it. "
                "Proceed without asking questions."
            ),
        )

        # Agent should have queried missing_in_tgt
        sql_calls = [tc for tc in result["tool_calls"] if tc["name"] == "execute_sql"]
        sql_texts = [tc["arguments"].get("statement", "") for tc in sql_calls]
        sql_upper = [s.upper() for s in sql_texts]

        assert any("MISSING_IN_TGT" in s for s in sql_upper), (
            f"Expected query on missing_in_tgt table. SQL calls: {sql_texts}"
        )

        response = result["final_response"]
        response_lower = response.lower()

        # Should reference specific customer IDs from mock data (500, 750)
        found_ids = "500" in response or "750" in response
        assert found_ids, (
            f"Should reference specific customer IDs (500 or 750). "
            f"Response:\n{response[:500]}"
        )

        # Should suggest remediation
        has_suggestion = (
            "re-run" in response_lower
            or "re-migrate" in response_lower
            or "migration" in response_lower
            or "insert" in response_lower
            or "fix" in response_lower
            or "log" in response_lower
        )
        assert has_suggestion, (
            f"Should suggest how to fix missing rows. Response:\n{response[:500]}"
        )


@pytest.mark.tier1
class TestColumnMismatches:
    """Test 12: Diagnose column mismatches and suggest fixes."""

    def test_diagnose_mismatches_and_suggest_fix(self, run_agent):
        """Agent should query mismatch_data and suggest thresholds or transformations."""
        result = run_agent(
            user_prompt=(
                "My Lakebridge reconciliation (recon_id='abc123') shows mismatch_data "
                "for order_items. The unit_price values are slightly different between "
                "source and target due to decimal precision. "
                "Query remorph.reconcile.mismatch_data to see the differences and "
                "suggest config changes to fix this. "
                "Proceed without asking questions."
            ),
        )

        # Agent should have queried mismatch_data
        sql_calls = [tc for tc in result["tool_calls"] if tc["name"] == "execute_sql"]
        sql_texts = [tc["arguments"].get("statement", "") for tc in sql_calls]
        sql_upper = [s.upper() for s in sql_texts]

        assert any("MISMATCH_DATA" in s for s in sql_upper), (
            f"Expected query on mismatch_data table. SQL calls: {sql_texts}"
        )

        response = result["final_response"]
        response_lower = response.lower()

        # Should suggest column_thresholds or transformations
        has_threshold_suggestion = (
            "threshold" in response_lower
            or "column_threshold" in response_lower
        )
        has_transform_suggestion = (
            "transformation" in response_lower
            or "transform" in response_lower
            or "coalesce" in response_lower
            or "cast" in response_lower
        )
        assert has_threshold_suggestion or has_transform_suggestion, (
            f"Should suggest column_thresholds or transformations. "
            f"Response:\n{response[:500]}"
        )

        # Should mention the unit_price column
        assert "unit_price" in response_lower, (
            f"Should reference unit_price column. Response:\n{response[:500]}"
        )

        # Should provide a concrete config snippet or specific bounds
        has_concrete_fix = (
            "0.01" in response
            or "0.001" in response
            or "lower_bound" in response_lower
            or "upper_bound" in response_lower
            or "decimal" in response_lower
            or "```" in response  # code/config snippet
        )
        assert has_concrete_fix, (
            f"Should provide concrete config snippet with bounds. "
            f"Response:\n{response[:500]}"
        )
