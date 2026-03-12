"""Generates an asciicast demo of the Lakebridge Reconcile config generation.

Runs the real agent, validates output, and produces a .cast file
that mimics a Claude Code session — playable with `asciinema play`.
"""

from pathlib import Path

import pytest
import yaml

from tests.tier1.lakebridge_reconcile.asciicast_recorder import AsciicastRecorder
from tests.tier1.lakebridge_reconcile.config_validator import extract_yaml_blocks

DEMOS_DIR = Path(__file__).parents[3] / "demos"


def _find_reconcile_yml(yaml_blocks: list[str]) -> str | None:
    for block in yaml_blocks:
        if "data_source" in block and "report_type" in block:
            return block
    return None


def _find_recon_config_yml(yaml_blocks: list[str]) -> str | None:
    for block in yaml_blocks:
        if "tables:" in block and ("source_name" in block or "target_name" in block):
            return block
    return None


def _build_config_cast(
    reconcile_data: dict, recon_config_data: dict
) -> AsciicastRecorder:
    """Build a ~10-second asciicast showing config generation in Claude Code style."""
    rec = AsciicastRecorder(
        width=90,
        height=32,
        title="Lakebridge Reconcile — Config Generation",
    )

    # ── Section 1: Context comments (0.0 – 0.8s) ──
    rec.comment("Lakebridge Reconcile skill demo", delay=0.4)
    rec.comment("Generating YAML config for a Snowflake → Databricks migration", delay=0.4)
    rec.newline()

    # ── Section 2: User prompt (0.8 – 2.0s) ──
    rec.prompt("Generate reconcile YAML configs for Snowflake (PROD_DB.PUBLIC)", delay=0.05)
    rec.prompt_cont("→ Databricks (main.migrated). Tables: customers, orders,", delay=0.05)
    rec.prompt_cont("order_items, products. report_type 'all'.", delay=0.05)
    rec.newline()
    rec.pause(1.0)

    # ── Section 3: Assistant header (2.0 – 2.5s) ──
    rec.assistant_header("Generating reconcile.yml and recon_config.yml …")
    rec.newline()
    rec.pause(0.4)

    # ── Section 4: reconcile.yml (2.5 – 5.0s) ──
    rec.comment("reconcile.yml — source, target, and report settings", delay=0.2)

    ds = reconcile_data.get("data_source", "snowflake")
    rt = reconcile_data.get("report_type", "all")
    ss = reconcile_data.get("secret_scope", "lakebridge_snowflake")
    db = reconcile_data.get("database_config", {})

    rec.yaml_line("version: 1")
    rec.yaml_line(f"data_source: {ds}")
    rec.yaml_line(f"report_type: {rt}")
    rec.yaml_line(f"secret_scope: {ss}")
    rec.yaml_line("database_config:")
    rec.yaml_line(f"  source_schema: {db.get('source_schema', 'PROD_DB.PUBLIC')}")
    rec.yaml_line(f"  target_catalog: {db.get('target_catalog', 'main')}")
    rec.yaml_line(f"  target_schema: {db.get('target_schema', 'migrated')}")
    rec.newline()
    rec.pause(1.2)

    # ── Section 5: recon_config.yml (5.0 – 8.5s) ──
    rec.comment("recon_config.yml — per-table reconciliation rules", delay=0.2)

    tables = recon_config_data.get("tables", [])
    rec.yaml_line("version: 2")
    rec.yaml_line("tables:")

    for table in tables[:2]:
        sn = table.get("source_name", "table")
        tn = table.get("target_name", sn)
        jc = table.get("join_columns", ["id"])
        jc_str = ", ".join(jc) if isinstance(jc, list) else str(jc)
        rec.yaml_line(f"  - source_name: {sn}")
        rec.yaml_line(f"    target_name: {tn}")
        rec.yaml_line(f"    join_columns: [{jc_str}]")

    remaining = len(tables) - 2
    if remaining > 0:
        rec.yaml_line(f"  # … {remaining} more table(s)")

    rec.newline()
    rec.pause(1.0)

    # ── Section 6: Success (8.5 – 10.0s) ──
    rec.success(f"Valid configuration generated for {len(tables)} tables")
    rec.pause(1.5)

    return rec


@pytest.mark.tier1
class TestAsciicastConfigGeneration:
    """Generate an asciicast demo for the config generation scenario."""

    def test_config_generation_cast(self, run_agent):
        """Run config generation agent and produce a .cast demo file."""
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
            f"Expected at least 2 YAML blocks. Got {len(yaml_blocks)}."
        )

        reconcile_yml = _find_reconcile_yml(yaml_blocks)
        assert reconcile_yml is not None, "Missing reconcile.yml block"
        recon_config_yml = _find_recon_config_yml(yaml_blocks)
        assert recon_config_yml is not None, "Missing recon_config.yml block"

        reconcile_data = yaml.safe_load(reconcile_yml)
        recon_config_data = yaml.safe_load(recon_config_yml)

        # Build and write the asciicast
        cast = _build_config_cast(reconcile_data, recon_config_data)
        output_path = cast.write(DEMOS_DIR / "lakebridge_config_generation.cast")

        assert output_path.exists(), f"Cast file not written to {output_path}"
        assert cast.duration <= 11.0, (
            f"Cast too long: {cast.duration:.1f}s (max 10s)"
        )
