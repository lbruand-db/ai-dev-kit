# databricks-lakebridge-reconcile — Skill Specification

## Purpose

Provide a Claude Code skill that helps users configure and run [Lakebridge](https://github.com/databrickslabs/lakebridge) reconciliation to validate data after migration. The skill generates YAML configuration files (`reconcile.yml` + `recon_config.yml`), Python notebook code using the `databricks-labs-lakebridge` library, and guides users through the reconciliation workflow.

## What is Lakebridge Reconcile

Lakebridge is a Databricks Labs open-source project (v0.12.2+) that accelerates migrations to Databricks. Its **reconcile** module identifies discrepancies between a source system and a Databricks target by comparing schema, row-level hashes, and column-level data.

Key facts:
- **Supported sources**: Snowflake, Oracle, MS SQL Server, Synapse, Databricks
- **Target**: Always Databricks (Unity Catalog)
- **Report types**: `schema`, `row`, `data`, `all`
- **Execution**: Runs as a Databricks notebook via `TriggerReconService.trigger_recon()`
- **Credentials**: Stored in Databricks secret scopes (e.g., `lakebridge_snowflake`, `lakebridge_oracle`, `lakebridge_mssql`)
- **Cluster support**: Auto-detects serverless (uses UC volumes) vs standard (uses DataFrame caching)
- **Output**: Metadata tables + AI/BI Dashboard for drill-down analysis

## Report Types

| Type | What it does | Outputs |
|------|-------------|---------|
| `schema` | Compares schema of source and target | schema_comparison, schema_difference |
| `row` | Hash-based row matching (no join columns needed) | missing_in_src, missing_in_tgt |
| `data` | Row + column-level reconciliation using join_columns | mismatch_data, missing_in_src, missing_in_tgt, threshold_mismatch, mismatch_columns |
| `all` | Combines `data` + `schema` | All of the above |

## Configuration Model

Lakebridge reconciliation uses **two YAML files**, both stored in the `.lakebridge` installation folder in the user's workspace home directory. They are loaded by `databricks.labs.blueprint.installation.Installation`.

### File 1: `reconcile.yml` — Global reconcile settings

Defines the source type, report type, credentials, and database mapping. One file per reconciliation setup.

**Python class**: `ReconcileConfig` (from `databricks.labs.lakebridge.config`)

```yaml
# reconcile.yml (version 1)
version: 1
data_source: snowflake          # snowflake | oracle | mssql | synapse | databricks
report_type: all                 # schema | row | data | all
secret_scope: lakebridge_snowflake

database_config:
  source_catalog: source_db      # optional (some sources don't have catalogs)
  source_schema: public
  target_catalog: main
  target_schema: migrated

metadata_config:
  catalog: remorph               # where reconcile metadata tables are stored (default: remorph)
  schema: reconcile              # metadata schema (default: reconcile)
  volume: reconcile_volume       # UC volume for intermediate data on serverless (default: reconcile_volume)

# optional — only needed for job-based execution
job_overrides:
  existing_cluster_id: "0123-456789-abcdef"
  tags:
    team: data-engineering
    project: migration-q1
```

#### `reconcile.yml` field reference

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `version` | int | Yes | Schema version, currently `1` |
| `data_source` | string | Yes | Source system: `snowflake`, `oracle`, `mssql`, `synapse`, `databricks` |
| `report_type` | string | Yes | `schema`, `row`, `data`, or `all` |
| `secret_scope` | string | Yes | Databricks secret scope holding source credentials |
| `database_config.source_catalog` | string | No | Source catalog (omit if source has no catalog concept) |
| `database_config.source_schema` | string | Yes | Source schema |
| `database_config.target_catalog` | string | Yes | Unity Catalog target catalog |
| `database_config.target_schema` | string | Yes | Unity Catalog target schema |
| `metadata_config.catalog` | string | No | Catalog for reconcile metadata tables (default: `remorph`) |
| `metadata_config.schema` | string | No | Schema for reconcile metadata tables (default: `reconcile`) |
| `metadata_config.volume` | string | No | UC volume for intermediate data on serverless (default: `reconcile_volume`) |
| `job_overrides.existing_cluster_id` | string | No | Cluster ID for job execution |
| `job_overrides.tags` | dict | No | Custom tags for the reconciliation job |

#### Default secret scopes per source

| Source | Default scope | Auth notes |
|--------|--------------|------------|
| Snowflake | `lakebridge_snowflake` | Prefers encrypted PEM private key over password |
| Oracle | `lakebridge_oracle` | |
| MS SQL Server | `lakebridge_mssql` | |
| Synapse | `lakebridge_synapse` | |
| Databricks | _(none needed)_ | Both sides are Databricks |

### File 2: `recon_config.yml` — Table-level reconciliation definitions

Defines which tables to compare and how. The actual filename loaded at runtime follows this pattern:

```
recon_config_[DATA_SOURCE]_[SOURCE_CATALOG_OR_SCHEMA]_[REPORT_TYPE].json
```

**Python class**: `TableRecon` (from `databricks.labs.lakebridge.config`, wraps a list of `Table` from `recon_config.py`)

```yaml
# recon_config.yml (version 2)
version: 2
tables:
  - source_name: product_prod
    target_name: product
    join_columns:
      - p_id
    select_columns:
      - id
      - name
      - price
    drop_columns:
      - comment
    column_mapping:
      - source_name: p_id
        target_name: product_id
      - source_name: p_name
        target_name: product_name
    transformations:
      - column_name: creation_date
        source: "creation_date"
        target: "to_date(creation_date,'yyyy-mm-dd')"
      - column_name: unit_price
        source: "coalesce(cast(cast(unit_price as decimal(38,10)) as string), '_null_recon_')"
        target: "coalesce(cast(format_number(cast(unit_price as decimal(38,10)), 10) as string), '_null_recon_')"
    column_thresholds:
      - column_name: price
        lower_bound: "-5%"
        upper_bound: "5%"
        type: float
    table_thresholds:
      - lower_bound: "0%"
        upper_bound: "5%"
        model: mismatch
    filters:
      source: "p_id > 0"
      target: "product_id > 0"
    jdbc_reader_options:
      number_partitions: 10
      partition_column: p_id
      lower_bound: "0"
      upper_bound: "10000000"
      fetch_size: 10000
    aggregates:
      - type: MIN
        agg_columns: [discount]
        group_by_columns: [p_id]
      - type: MAX
        agg_columns: [price]
    # optional — sampling for large tables
    sampling_options:
      method: stratified            # stratified
      specifications:
        type: rows                  # rows (fraction is currently disabled)
        value: 10000
      stratified_columns: [region]
      stratified_buckets: 10
```

#### `recon_config.yml` table field reference

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source_name` | string | Yes | Source table name (auto-lowercased) |
| `target_name` | string | Yes | Target table name (auto-lowercased) |
| `join_columns` | list[string] | For `data`/`all` | Primary key columns for row matching |
| `select_columns` | list[string] | No | Include only these columns |
| `drop_columns` | list[string] | No | Exclude these columns |
| `column_mapping` | list[{source_name, target_name}] | No | Map differing column names between source/target |
| `transformations` | list[{column_name, source, target}] | No | Dialect-specific SQL expressions applied before comparison |
| `column_thresholds` | list[{column_name, lower_bound, upper_bound, type}] | No | Per-column acceptable variance (% or absolute) |
| `table_thresholds` | list[{lower_bound, upper_bound, model}] | No | Table-level mismatch tolerance (model must be `mismatch`) |
| `filters` | {source, target} | No | WHERE clauses applied to each side |
| `jdbc_reader_options` | {number_partitions, partition_column, lower_bound, upper_bound, fetch_size} | No | JDBC parallel read tuning |
| `aggregates` | list[{type, agg_columns, group_by_columns}] | No | Aggregate metric reconciliation |
| `sampling_options` | {method, specifications, stratified_columns, stratified_buckets} | No | Sampling for large tables |

#### JDBC reader options detail

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `number_partitions` | int | Yes (if used) | Number of parallel partitions |
| `partition_column` | string | Yes (if used) | High-cardinality column for distribution |
| `lower_bound` | string | Yes (if used) | Distribution range minimum |
| `upper_bound` | string | Yes (if used) | Distribution range maximum |
| `fetch_size` | int | No | Rows per JDBC round-trip (default: 100) |

#### Sampling options detail

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `method` | string | Yes (if used) | `stratified` |
| `specifications.type` | string | Yes (if used) | `rows` (note: `fraction` is currently disabled) |
| `specifications.value` | float | Yes (if used) | Number of rows to sample |
| `stratified_columns` | list[string] | For stratified | Columns to stratify by |
| `stratified_buckets` | int | For stratified | Number of buckets |

### Important configuration rules

- **All column names are auto-lowercased** by Lakebridge during parsing
- Column references in configs use **source column names** except in `transformations` and `filters`
- NULLs must be handled explicitly in transformations (e.g., `coalesce(col, '_null_recon_')`)
- Timestamps: convert to unix epoch using dialect-appropriate functions (`epoch_millisecond` for Snowflake, `unix_millis` for Databricks)
- `column_thresholds.type` must align with SQLGLOT `DataType.NUMERIC_TYPES` or `DataType.TEMPORAL_TYPES`
- `column_thresholds` support both percentage (`"-5%"`) and absolute (`"-50"`) bounds
- `table_thresholds` bounds must be non-negative, `lower_bound <= upper_bound`, and `model` must be `"mismatch"`
- `select_columns` and `drop_columns` are **not** applied to aggregate reconciliation
- Supported aggregate functions: min, max, count, sum, avg, mean, mode, stddev, variance, median

## Execution

### Notebook code

```python
%pip install databricks-labs-lakebridge
dbutils.library.restartPython()

from databricks.sdk import WorkspaceClient
from databricks.labs.lakebridge.config import (
    ReconcileConfig, DatabaseConfig, ReconcileMetadataConfig, TableRecon
)
from databricks.labs.lakebridge.reconcile.recon_config import (
    Table, ColumnMapping, Transformation,
    ColumnThresholds, TableThresholds, Filters, JdbcReaderOptions, Aggregate
)
from databricks.labs.lakebridge.reconcile.trigger_recon_service import TriggerReconService

ws = WorkspaceClient()

table_recon = TableRecon(tables=[
    Table(
        source_name="product_prod",
        target_name="product",
        join_columns=["p_id"],
        column_mapping=[ColumnMapping(source_name="p_id", target_name="product_id")],
        # ... additional per-table config
    )
])

reconcile_config = ReconcileConfig(
    data_source="snowflake",        # snowflake | oracle | mssql | synapse | databricks
    report_type="all",              # schema | row | data | all
    secret_scope="lakebridge_snowflake",
    database_config=DatabaseConfig(
        source_catalog="source_db",
        source_schema="public",
        target_catalog="main",
        target_schema="migrated"
    ),
    metadata_config=ReconcileMetadataConfig(
        catalog="remorph",          # default
        schema="reconcile",         # default
        volume="reconcile_volume"   # default
    )
)

result = TriggerReconService.trigger_recon(
    ws=ws,
    spark=spark,
    table_recon=table_recon,
    reconcile_config=reconcile_config
)
# result contains recon_id for tracking and drill-down in the AI/BI Dashboard
```

#### CLI-based execution (alternative)

Lakebridge can also run reconciliation as a job entry point via `databricks.labs.lakebridge.reconcile.execute.main()`. In this mode it loads `reconcile.yml` and the matching `recon_config_*.json` automatically from the `.lakebridge` installation folder:

```bash
# The entry point expects: operation_name [install_folder]
# operation_name: "reconcile" or "aggregates-reconcile"
python -m databricks.labs.lakebridge.reconcile.execute reconcile
python -m databricks.labs.lakebridge.reconcile.execute aggregates-reconcile
```

### Automation

Lakebridge provides three notebooks for automated reconciliation:

1. **recon_wrapper_nb** — Orchestrator: reads table configs, triggers reconciliation per table
2. **lakebridge_recon_main** — Performs row, column, and schema comparisons
3. **transformation_query_generator** — Applies source-specific transformations for hash computation

Automation uses two lookup tables:
- **table_configs** — Stores table pairs, filters, transformations, thresholds, primary keys
- **table_recon_summary** — Records validation results with timestamps, status, recon IDs

### Permissions required

- Permission to create SQL Warehouses
- `USE CATALOG` and `CREATE SCHEMA` on the metadata catalog
- `CREATE VOLUMES` if using pre-existing schemas

## Scope of this Skill

### In Scope

- Generating Lakebridge `reconcile.yml` and `recon_config.yml` files from user descriptions of their tables
- Generating Python notebook code that uses `TriggerReconService`
- Generating the automation notebook trio (wrapper + main + transformer) for multi-table reconciliation
- Guiding users through report type selection (`schema`, `row`, `data`, `all`)
- Helping configure column mappings, transformations, thresholds, and filters
- JDBC reader options tuning for large source tables
- Aggregate reconciliation configuration
- Secret scope setup guidance
- Interpreting reconciliation output tables and suggesting fixes for mismatches
- Validating generated YAML configs against the Lakebridge schema before deployment

### Out of Scope

- Performing the actual migration (Lakebridge transpile/assessment modules)
- Real-time / streaming reconciliation
- Automated remediation of mismatches
- Sources not supported by Lakebridge (e.g., PostgreSQL, MySQL, Parquet files)
- Scheduling via Databricks Workflows (not yet documented by Lakebridge)

## User Scenarios

### Scenario 1: Full Post-Migration Validation

A user migrated 50 tables from Snowflake to Unity Catalog. They want to generate `reconcile.yml` + `recon_config.yml` that run all checks (schema + data) across every table, with appropriate column mappings and JDBC reader options.

### Scenario 2: Schema-Only Check

A user wants a quick schema comparison before running expensive data reconciliation. The skill generates a `schema`-type config for their Oracle source.

### Scenario 3: Data Reconciliation with Thresholds

A user has floating-point price columns that may differ slightly due to precision changes. They need `column_thresholds` configured with appropriate bounds and `transformations` to normalize decimal formatting.

### Scenario 4: Aggregate Validation

A user wants to verify that SUM, MIN, MAX of key numeric columns match between source and target, grouped by business dimensions.

### Scenario 5: Filtering Subsets

A user only wants to reconcile recent data (e.g., last 30 days). The skill generates appropriate `filters` for both source and target using dialect-specific date functions.

## File Structure (Planned)

```
databricks-lakebridge-reconcile/
  SKILL.md              — Skill definition, trigger conditions, and usage patterns
  SPEC.md               — This file
  configuration.md      — Deep-dive on all config fields, gotchas, and transformation patterns
  examples.md           — Complete end-to-end examples per source type
  secret_scopes.md      — Secret scope setup per source (Snowflake, Oracle, MSSQL, Synapse)
```

## Resolved Decisions

- **Interpret reconciliation output?** Yes — the skill should help users read output tables and suggest fixes
- **Automation notebooks?** Yes — generate the full trio (wrapper + main + transformer), not just single-table configs
- **AI/BI Dashboard integration?** No — not in scope
- **Config validation?** Yes — validate generated YAML configs against the Lakebridge dataclass schema before deployment