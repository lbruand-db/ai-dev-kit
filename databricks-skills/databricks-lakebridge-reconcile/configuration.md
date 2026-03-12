# Lakebridge Reconcile Configuration Reference

## Two-File Configuration Model

Lakebridge reconciliation uses two YAML files in the `.lakebridge` workspace folder, loaded by `databricks.labs.blueprint.installation.Installation`.

## File 1: `reconcile.yml`

Global settings — one per reconciliation setup.

**Python class**: `ReconcileConfig` from `databricks.labs.lakebridge.config`

```yaml
version: 1
data_source: snowflake          # snowflake | oracle | mssql | synapse | databricks
report_type: all                # schema | row | data | all
secret_scope: lakebridge_snowflake

database_config:
  source_catalog: prod_db       # optional — omit if source has no catalog concept
  source_schema: public
  target_catalog: main
  target_schema: migrated

metadata_config:
  catalog: remorph              # default: remorph
  schema: reconcile             # default: reconcile
  volume: reconcile_volume      # default: reconcile_volume (used on serverless)

# optional — for job-based execution only
job_overrides:
  existing_cluster_id: "0123-456789-abcdef"
  tags:
    team: data-engineering
```

### `reconcile.yml` field reference

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `version` | int | Yes | — | Schema version, currently `1` |
| `data_source` | string | Yes | — | `snowflake`, `oracle`, `mssql`, `synapse`, `databricks` |
| `report_type` | string | Yes | — | `schema`, `row`, `data`, `all` |
| `secret_scope` | string | Yes | — | Databricks secret scope with source credentials |
| `database_config.source_catalog` | string | No | `None` | Source catalog (omit for sources without catalogs) |
| `database_config.source_schema` | string | Yes | — | Source schema |
| `database_config.target_catalog` | string | Yes | — | Unity Catalog target catalog |
| `database_config.target_schema` | string | Yes | — | Unity Catalog target schema |
| `metadata_config.catalog` | string | No | `remorph` | Catalog for reconcile metadata tables |
| `metadata_config.schema` | string | No | `reconcile` | Schema for reconcile metadata tables |
| `metadata_config.volume` | string | No | `reconcile_volume` | UC volume for serverless intermediate data |
| `job_overrides.existing_cluster_id` | string | No | — | Cluster ID for job execution |
| `job_overrides.tags` | dict | No | — | Custom tags for the reconciliation job |

## File 2: `recon_config.yml`

Per-table reconciliation definitions. At runtime the filename follows:

```
recon_config_[DATA_SOURCE]_[SOURCE_CATALOG_OR_SCHEMA]_[REPORT_TYPE].json
```

**Python class**: `TableRecon` from `databricks.labs.lakebridge.config`, containing a list of `Table` from `databricks.labs.lakebridge.reconcile.recon_config`

```yaml
version: 2
tables:
  - source_name: product_prod
    target_name: product
    join_columns: [p_id]
    select_columns: [id, name, price]
    drop_columns: [comment]
    column_mapping:
      - source_name: p_id
        target_name: product_id
      - source_name: p_name
        target_name: product_name
    transformations:
      - column_name: creation_date
        source: "creation_date"
        target: "to_date(creation_date,'yyyy-mm-dd')"
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
    sampling_options:
      method: stratified
      specifications:
        type: rows
        value: 10000
      stratified_columns: [region]
      stratified_buckets: 10
```

### Table field reference

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source_name` | string | Yes | Source table name (auto-lowercased) |
| `target_name` | string | Yes | Target table name (auto-lowercased) |
| `join_columns` | list[string] | For `data`/`all` | Primary key columns for row matching |
| `select_columns` | list[string] | No | Include only these columns (auto-lowercased) |
| `drop_columns` | list[string] | No | Exclude these columns (auto-lowercased) |
| `column_mapping` | list | No | Map differing column names |
| `transformations` | list | No | Dialect-specific SQL expressions before comparison |
| `column_thresholds` | list | No | Per-column acceptable variance |
| `table_thresholds` | list | No | Table-level mismatch tolerance |
| `filters` | object | No | WHERE clauses per side |
| `jdbc_reader_options` | object | No | JDBC parallel read tuning |
| `aggregates` | list | No | Aggregate metric reconciliation |
| `sampling_options` | object | No | Sampling for large tables |

### Column mapping

Maps source column names to different target column names:

```yaml
column_mapping:
  - source_name: dept_id        # auto-lowercased
    target_name: department_id  # auto-lowercased
```

### Transformations

Apply dialect-specific SQL expressions before comparison. **Use source column names** in `column_name`, but dialect-appropriate SQL in `source`/`target` expressions.

```yaml
transformations:
  # Decimal precision normalization
  - column_name: unit_price
    source: "coalesce(cast(cast(unit_price as decimal(38,10)) as string), '_null_recon_')"
    target: "coalesce(cast(format_number(cast(unit_price as decimal(38,10)), 10) as string), '_null_recon_')"

  # Date format normalization
  - column_name: created_at
    source: "created_at"
    target: "to_date(created_at, 'yyyy-MM-dd')"

  # Timestamp to epoch (Snowflake → Databricks)
  - column_name: event_time
    source: "epoch_millisecond(event_time)"
    target: "unix_millis(event_time)"
```

**Critical rules:**
- Always handle NULLs explicitly: `coalesce(col, '_null_recon_')`
- Convert timestamps to unix epoch for cross-dialect comparison
- `source` and `target` fields are optional — omit one to apply transformation only on one side

### Column thresholds

Define acceptable variance per column. Supports percentage and absolute bounds.

```yaml
column_thresholds:
  # Percentage bounds
  - column_name: discount
    lower_bound: "-5%"
    upper_bound: "5%"
    type: float

  # Absolute bounds
  - column_name: quantity
    lower_bound: "-1"
    upper_bound: "1"
    type: int
```

`type` must match SQLGLOT `DataType.NUMERIC_TYPES` (int, float, decimal, etc.) or `DataType.TEMPORAL_TYPES` (date, timestamp, etc.).

### Table thresholds

Define table-level mismatch tolerance:

```yaml
table_thresholds:
  - lower_bound: "0%"    # must be non-negative
    upper_bound: "5%"    # must be >= lower_bound
    model: mismatch      # only valid value
```

### Filters

Apply WHERE clauses to source and/or target before comparison. Use dialect-appropriate SQL.

```yaml
# Filter by date range
filters:
  source: "order_date >= '2024-01-01'"
  target: "order_date >= '2024-01-01'"

# Filter by department (with lowering for case-insensitive match)
filters:
  source: "lower(dept_name) = 'finance'"
  target: "lower(dept_name) = 'finance'"
```

### JDBC reader options

Controls parallel reads from JDBC sources. Use for large tables.

```yaml
jdbc_reader_options:
  number_partitions: 200       # parallel partition count
  partition_column: order_id   # high-cardinality column (auto-lowercased)
  lower_bound: "1"             # distribution range minimum
  upper_bound: "100000000"     # distribution range maximum
  fetch_size: 10000            # rows per JDBC round-trip (default: 100)
```

**Tips:**
- Choose a high-cardinality numeric column for `partition_column`
- Set `number_partitions` based on cluster size (2-4x number of cores)
- Increase `fetch_size` for wide tables, decrease for narrow tables with many rows

### Aggregates

Compare aggregate metrics between source and target:

```yaml
aggregates:
  - type: SUM
    agg_columns: [revenue]
    group_by_columns: [region, quarter]
  - type: COUNT
    agg_columns: [order_id]
  - type: AVG
    agg_columns: [unit_price]
    group_by_columns: [product_category]
```

**Supported functions:** min, max, count, sum, avg, mean, mode, stddev, variance, median

**Important:** `select_columns` and `drop_columns` are **not** applied to aggregate reconciliation. Transformations **do** apply to both aggregate and group-by columns.

### Sampling options

Sample large tables instead of full comparison:

```yaml
sampling_options:
  method: stratified
  specifications:
    type: rows           # only "rows" is supported (fraction is disabled)
    value: 10000         # number of rows to sample
  stratified_columns: [region]
  stratified_buckets: 10
```

**Note:** `fraction` sampling type is currently disabled in Lakebridge. Use `rows` instead.

## Configuration Validation Rules

When generating configs, validate these constraints:

1. **All column names auto-lowercase** — Lakebridge lowercases all column names during parsing
2. **`join_columns` required for `data` and `all` report types** — omit only for `schema` or `row`
3. **`table_thresholds.model`** must be `"mismatch"` — no other value is accepted
4. **`table_thresholds` bounds** must be non-negative and `lower_bound <= upper_bound`
5. **`column_thresholds.type`** must be a valid SQLGLOT numeric or temporal type
6. **Aggregate `type`** must be one of: min, max, count, sum, avg, mean, mode, stddev, variance, median
7. **`sampling_options.method`** currently only supports `"stratified"`
8. **`sampling_options.specifications.type`** must be `"rows"` (fraction is disabled)
9. **Stratified sampling** requires both `stratified_columns` and `stratified_buckets`
