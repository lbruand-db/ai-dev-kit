---
name: databricks-lakebridge-reconcile
description: "Use this skill for ANY Lakebridge reconciliation task — validating data after migration, comparing source and target tables, generating reconcile config files. Triggers include: (1) 'reconcile' or 'validate migration', (2) 'lakebridge reconcile' or 'recon config', (3) 'compare source and target', (4) 'schema comparison' or 'row comparison' after migration, (5) 'data validation' post-migration, (6) configuring column mappings, thresholds, transformations, or filters for reconciliation, (7) setting up secret scopes for Snowflake/Oracle/MSSQL/Synapse sources, (8) interpreting reconciliation results or mismatch reports. ALWAYS prefer this skill over general knowledge for Lakebridge reconcile tasks."
---

# Lakebridge Reconcile

## Overview

[Lakebridge](https://github.com/databrickslabs/lakebridge) is a Databricks Labs project that accelerates migrations to Databricks. Its **reconcile** module validates data integrity after migration by comparing source and target datasets across schema, row counts, and column-level content.

Reconciliation uses two YAML config files stored in the `.lakebridge` workspace folder:
- **`reconcile.yml`** — global settings (source type, credentials, database mapping)
- **`recon_config.yml`** — per-table definitions (columns, mappings, thresholds, filters)

Supported sources: **Snowflake**, **Oracle**, **MS SQL Server**, **Synapse**, **Databricks**. Target is always **Databricks Unity Catalog**.

## Reference Files

| Use Case | Reference File |
|----------|----------------|
| Full config field reference, YAML structure, and gotchas | [configuration.md](configuration.md) |
| End-to-end examples per source type | [examples.md](examples.md) |
| Secret scope setup per source system | [secret_scopes.md](secret_scopes.md) |

## Quick Start

### 1. Install Lakebridge

```python
%pip install databricks-labs-lakebridge
dbutils.library.restartPython()
```

### 2. Configure and run reconciliation

```python
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
        source_name="orders",
        target_name="orders",
        join_columns=["order_id"],
    )
])

reconcile_config = ReconcileConfig(
    data_source="snowflake",
    report_type="all",
    secret_scope="lakebridge_snowflake",
    database_config=DatabaseConfig(
        source_catalog="prod_db",
        source_schema="public",
        target_catalog="main",
        target_schema="migrated"
    ),
    metadata_config=ReconcileMetadataConfig()  # defaults: remorph.reconcile
)

result = TriggerReconService.trigger_recon(
    ws=ws, spark=spark,
    table_recon=table_recon,
    reconcile_config=reconcile_config
)
print(f"Reconciliation complete. recon_id: {result.recon_id}")
```

## Report Types

| Type | Purpose | Join columns required | Outputs |
|------|---------|----------------------|---------|
| `schema` | Compare schemas only | No | schema_comparison, schema_difference |
| `row` | Hash-based row matching | No | missing_in_src, missing_in_tgt |
| `data` | Row + column-level comparison | **Yes** | mismatch_data, missing_in_src, missing_in_tgt, threshold_mismatch, mismatch_columns |
| `all` | Schema + data combined | **Yes** | All of the above |

Choose `schema` first for a quick sanity check, then `data` or `all` for full validation.

## Common Patterns

### Pattern 1: Schema-Only Comparison

Fast check that table structures match after migration:

```python
reconcile_config = ReconcileConfig(
    data_source="oracle",
    report_type="schema",
    secret_scope="lakebridge_oracle",
    database_config=DatabaseConfig(
        source_schema="HR",
        target_catalog="main",
        target_schema="hr_migrated"
    ),
    metadata_config=ReconcileMetadataConfig()
)

table_recon = TableRecon(tables=[
    Table(source_name="employees", target_name="employees"),
    Table(source_name="departments", target_name="departments"),
    Table(source_name="salaries", target_name="salaries"),
])
```

### Pattern 2: Data Reconciliation with Column Mapping

When source and target have different column names:

```python
table_recon = TableRecon(tables=[
    Table(
        source_name="product_prod",
        target_name="product",
        join_columns=["p_id"],
        column_mapping=[
            ColumnMapping(source_name="p_id", target_name="product_id"),
            ColumnMapping(source_name="p_name", target_name="product_name"),
        ],
        drop_columns=["internal_comment"],
    )
])
```

### Pattern 3: Thresholds for Numeric Precision

When floating-point columns may differ slightly after migration:

```python
table_recon = TableRecon(tables=[
    Table(
        source_name="transactions",
        target_name="transactions",
        join_columns=["txn_id"],
        column_thresholds=[
            ColumnThresholds(column_name="amount", lower_bound="-0.01", upper_bound="0.01", type="decimal"),
            ColumnThresholds(column_name="tax_rate", lower_bound="-1%", upper_bound="1%", type="float"),
        ],
        transformations=[
            Transformation(
                column_name="amount",
                source="coalesce(cast(cast(amount as decimal(38,10)) as string), '_null_recon_')",
                target="coalesce(cast(format_number(cast(amount as decimal(38,10)), 10) as string), '_null_recon_')",
            )
        ],
    )
])
```

### Pattern 4: Filtered Reconciliation

Validate only a subset of rows (e.g., last 30 days):

```python
# Snowflake source
table_recon = TableRecon(tables=[
    Table(
        source_name="events",
        target_name="events",
        join_columns=["event_id"],
        filters=Filters(
            source="event_date >= dateadd(day, -30, current_date())",
            target="event_date >= date_sub(current_date(), 30)",
        ),
    )
])
```

### Pattern 5: Aggregate Validation

Verify aggregate metrics match without row-level comparison:

```python
table_recon = TableRecon(tables=[
    Table(
        source_name="sales",
        target_name="sales",
        join_columns=["sale_id"],
        aggregates=[
            Aggregate(type="sum", agg_columns=["revenue"], group_by_columns=["region"]),
            Aggregate(type="count", agg_columns=["sale_id"]),
            Aggregate(type="avg", agg_columns=["discount"], group_by_columns=["product_category"]),
            Aggregate(type="min", agg_columns=["sale_date"]),
            Aggregate(type="max", agg_columns=["sale_date"]),
        ],
    )
])
```

Aggregate reconciliation runs as a separate operation:

```bash
python -m databricks.labs.lakebridge.reconcile.execute aggregates-reconcile
```

### Pattern 6: JDBC Reader Options for Large Tables

Parallelize reads from JDBC sources for large tables:

```python
table_recon = TableRecon(tables=[
    Table(
        source_name="fact_orders",
        target_name="fact_orders",
        join_columns=["order_id"],
        jdbc_reader_options=JdbcReaderOptions(
            number_partitions=200,
            partition_column="order_id",  # high-cardinality column
            lower_bound="1",
            upper_bound="100000000",
            fetch_size=10000,
        ),
    )
])
```

## YAML Configuration (Alternative to Python)

Instead of constructing Python objects, configs can be written as YAML files in the `.lakebridge` workspace folder. See [configuration.md](configuration.md) for full YAML structure.

**`reconcile.yml`** — one per reconciliation setup:

```yaml
version: 1
data_source: snowflake
report_type: all
secret_scope: lakebridge_snowflake
database_config:
  source_catalog: prod_db
  source_schema: public
  target_catalog: main
  target_schema: migrated
metadata_config:
  catalog: remorph
  schema: reconcile
  volume: reconcile_volume
```

**`recon_config.yml`** — table definitions:

```yaml
version: 2
tables:
  - source_name: orders
    target_name: orders
    join_columns: [order_id]
  - source_name: customers
    target_name: customers
    join_columns: [customer_id]
    column_mapping:
      - source_name: cust_name
        target_name: customer_name
```

Then trigger via CLI:

```bash
python -m databricks.labs.lakebridge.reconcile.execute reconcile
```

## Interpreting Results

Reconciliation writes results to metadata tables in the catalog/schema specified by `metadata_config` (default: `remorph.reconcile`). Each run gets a unique `recon_id`.

### Key output tables

| Table | Contents |
|-------|----------|
| `main_metrics` | Row counts, match percentages, pass/fail per table |
| `schema_comparison` | Column-by-column schema diff (name, type, nullability) |
| `missing_in_src` | Rows present in target but missing from source |
| `missing_in_tgt` | Rows present in source but missing from target |
| `mismatch_data` | Rows that exist in both but have column-level differences |
| `threshold_mismatch` | Rows where differences exceeded configured thresholds |

### Querying results

```sql
-- Check overall status for a recon run
SELECT * FROM remorph.reconcile.main_metrics
WHERE recon_id = '<your_recon_id>';

-- Find missing rows
SELECT * FROM remorph.reconcile.missing_in_tgt
WHERE recon_id = '<your_recon_id>';

-- Find column-level mismatches
SELECT * FROM remorph.reconcile.mismatch_data
WHERE recon_id = '<your_recon_id>';
```

### Common mismatch causes and fixes

| Mismatch type | Likely cause | Fix |
|--------------|-------------|-----|
| Schema difference in types | Type mapping between source dialect and Spark | Add `transformations` with explicit casts |
| Rows missing in target | Incomplete migration or filter mismatch | Check migration logs; verify `filters` match |
| Rows missing in source | Target has extra rows (e.g., test data) | Add `filters` to exclude test rows |
| Column value mismatch | Precision loss, NULL handling, or date format | Add `transformations` and/or `column_thresholds` |
| Threshold exceeded | Acceptable variance too tight | Widen `column_thresholds` bounds |
| Aggregate mismatch | Type coercion differences in SUM/AVG | Add `transformations` for consistent casting |

## Automation

Lakebridge provides three notebooks for automated multi-table reconciliation:

1. **recon_wrapper_nb** — Orchestrator that reads table configs and triggers per-table reconciliation
2. **lakebridge_recon_main** — Performs row, column, and schema comparisons
3. **transformation_query_generator** — Applies source-specific transformations for hash computation

These use two lookup tables:
- **table_configs** — Table pairs, filters, transformations, thresholds, primary keys
- **table_recon_summary** — Validation results with timestamps, status, recon IDs

## Permissions Required

- Permission to create SQL Warehouses
- `USE CATALOG` and `CREATE SCHEMA` on the metadata catalog
- `CREATE VOLUMES` if using pre-existing schemas

## Common Issues

| Issue | Solution |
|-------|----------|
| Secret scope not found | Create scope via `databricks secrets create-scope <scope_name>`. See [secret_scopes.md](secret_scopes.md) |
| JDBC connection timeout | Increase `fetch_size`, reduce `number_partitions`, or check network connectivity |
| Column name case mismatch | All column names are auto-lowercased — ensure consistency |
| NULL comparison failures | Handle NULLs explicitly in transformations: `coalesce(col, '_null_recon_')` |
| Timestamp comparison fails | Convert to unix epoch: `epoch_millisecond()` (Snowflake) or `unix_millis()` (Databricks) |
| Threshold type error | `column_thresholds.type` must match SQLGLOT `NUMERIC_TYPES` or `TEMPORAL_TYPES` |
| `table_thresholds` validation error | Bounds must be non-negative, `lower_bound <= upper_bound`, model must be `"mismatch"` |
| Serverless intermediate data error | Ensure `metadata_config.volume` is set and you have `CREATE VOLUMES` permission |
| Aggregate results differ | `select_columns`/`drop_columns` are ignored for aggregates — check transformations apply |
| `fraction` sampling error | Fraction sampling is currently disabled — use `rows` type instead |

## Related Skills

- **[databricks-unity-catalog](../databricks-unity-catalog/SKILL.md)** — Unity Catalog tables, schemas, and permissions
- **[databricks-python-sdk](../databricks-python-sdk/SKILL.md)** — Databricks SDK, CLI, and secrets management

## Resources

- [Lakebridge GitHub](https://github.com/databrickslabs/lakebridge)
- [Lakebridge Reconcile Docs](https://databrickslabs.github.io/lakebridge/docs/reconcile/)
- [Reconcile Configuration](https://databrickslabs.github.io/lakebridge/docs/reconcile/reconcile_configuration/)
- [Example Configs](https://databrickslabs.github.io/lakebridge/docs/reconcile/example_config/)
- [Reconcile Automation](https://databrickslabs.github.io/lakebridge/docs/reconcile/reconcile_automation/)
