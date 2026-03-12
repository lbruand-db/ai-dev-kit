# Lakebridge Reconcile — End-to-End Examples

## Example 1: Snowflake Full Reconciliation

Validate all tables migrated from Snowflake to Unity Catalog.

### reconcile.yml

```yaml
version: 1
data_source: snowflake
report_type: all
secret_scope: lakebridge_snowflake
database_config:
  source_catalog: PROD_DB
  source_schema: PUBLIC
  target_catalog: main
  target_schema: snowflake_migrated
metadata_config:
  catalog: remorph
  schema: reconcile
  volume: reconcile_volume
```

### recon_config.yml

```yaml
version: 2
tables:
  - source_name: customers
    target_name: customers
    join_columns: [customer_id]
    column_mapping:
      - source_name: cust_name
        target_name: customer_name
      - source_name: cust_email
        target_name: email
    transformations:
      - column_name: created_at
        source: "epoch_millisecond(created_at)"
        target: "unix_millis(created_at)"
    column_thresholds:
      - column_name: credit_limit
        lower_bound: "-0.01"
        upper_bound: "0.01"
        type: decimal

  - source_name: orders
    target_name: orders
    join_columns: [order_id]
    jdbc_reader_options:
      number_partitions: 100
      partition_column: order_id
      lower_bound: "1"
      upper_bound: "50000000"
      fetch_size: 5000
    table_thresholds:
      - lower_bound: "0%"
        upper_bound: "1%"
        model: mismatch

  - source_name: order_items
    target_name: order_items
    join_columns: [order_id, item_id]
    transformations:
      - column_name: unit_price
        source: "coalesce(cast(cast(unit_price as decimal(38,10)) as string), '_null_recon_')"
        target: "coalesce(cast(format_number(cast(unit_price as decimal(38,10)), 10) as string), '_null_recon_')"
    column_thresholds:
      - column_name: unit_price
        lower_bound: "-0.001"
        upper_bound: "0.001"
        type: decimal
      - column_name: discount
        lower_bound: "-1%"
        upper_bound: "1%"
        type: float
```

### Notebook

```python
%pip install databricks-labs-lakebridge
dbutils.library.restartPython()

from databricks.sdk import WorkspaceClient
from databricks.labs.lakebridge.config import (
    ReconcileConfig, DatabaseConfig, ReconcileMetadataConfig, TableRecon
)
from databricks.labs.lakebridge.reconcile.recon_config import (
    Table, ColumnMapping, Transformation, ColumnThresholds,
    TableThresholds, JdbcReaderOptions
)
from databricks.labs.lakebridge.reconcile.trigger_recon_service import TriggerReconService

ws = WorkspaceClient()

table_recon = TableRecon(tables=[
    Table(
        source_name="customers",
        target_name="customers",
        join_columns=["customer_id"],
        column_mapping=[
            ColumnMapping(source_name="cust_name", target_name="customer_name"),
            ColumnMapping(source_name="cust_email", target_name="email"),
        ],
        transformations=[
            Transformation(
                column_name="created_at",
                source="epoch_millisecond(created_at)",
                target="unix_millis(created_at)",
            )
        ],
        column_thresholds=[
            ColumnThresholds(column_name="credit_limit", lower_bound="-0.01", upper_bound="0.01", type="decimal")
        ],
    ),
    Table(
        source_name="orders",
        target_name="orders",
        join_columns=["order_id"],
        jdbc_reader_options=JdbcReaderOptions(
            number_partitions=100, partition_column="order_id",
            lower_bound="1", upper_bound="50000000", fetch_size=5000,
        ),
        table_thresholds=[
            TableThresholds(lower_bound="0%", upper_bound="1%", model="mismatch")
        ],
    ),
    Table(
        source_name="order_items",
        target_name="order_items",
        join_columns=["order_id", "item_id"],
        transformations=[
            Transformation(
                column_name="unit_price",
                source="coalesce(cast(cast(unit_price as decimal(38,10)) as string), '_null_recon_')",
                target="coalesce(cast(format_number(cast(unit_price as decimal(38,10)), 10) as string), '_null_recon_')",
            )
        ],
        column_thresholds=[
            ColumnThresholds(column_name="unit_price", lower_bound="-0.001", upper_bound="0.001", type="decimal"),
            ColumnThresholds(column_name="discount", lower_bound="-1%", upper_bound="1%", type="float"),
        ],
    ),
])

reconcile_config = ReconcileConfig(
    data_source="snowflake",
    report_type="all",
    secret_scope="lakebridge_snowflake",
    database_config=DatabaseConfig(
        source_catalog="PROD_DB",
        source_schema="PUBLIC",
        target_catalog="main",
        target_schema="snowflake_migrated",
    ),
    metadata_config=ReconcileMetadataConfig(),
)

result = TriggerReconService.trigger_recon(
    ws=ws, spark=spark, table_recon=table_recon, reconcile_config=reconcile_config
)
print(f"Done. recon_id: {result.recon_id}")
```

---

## Example 2: Oracle Schema-Only Check

Quick schema comparison before investing in full data reconciliation.

### Notebook

```python
from databricks.labs.lakebridge.config import (
    ReconcileConfig, DatabaseConfig, ReconcileMetadataConfig, TableRecon
)
from databricks.labs.lakebridge.reconcile.recon_config import Table
from databricks.labs.lakebridge.reconcile.trigger_recon_service import TriggerReconService
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()

table_recon = TableRecon(tables=[
    Table(source_name="employees", target_name="employees"),
    Table(source_name="departments", target_name="departments"),
    Table(source_name="job_history", target_name="job_history"),
])

reconcile_config = ReconcileConfig(
    data_source="oracle",
    report_type="schema",
    secret_scope="lakebridge_oracle",
    database_config=DatabaseConfig(
        source_schema="HR",
        target_catalog="main",
        target_schema="hr_migrated",
    ),
    metadata_config=ReconcileMetadataConfig(),
)

result = TriggerReconService.trigger_recon(
    ws=ws, spark=spark, table_recon=table_recon, reconcile_config=reconcile_config
)
```

### Check schema differences

```sql
SELECT table_name, column_name, source_datatype, target_datatype, is_valid
FROM remorph.reconcile.schema_comparison
WHERE recon_id = '<recon_id>'
  AND is_valid = false;
```

---

## Example 3: SQL Server with Filtered Subset

Reconcile only last 90 days of data from SQL Server.

### reconcile.yml

```yaml
version: 1
data_source: mssql
report_type: data
secret_scope: lakebridge_mssql
database_config:
  source_schema: dbo
  target_catalog: main
  target_schema: mssql_migrated
metadata_config:
  catalog: remorph
  schema: reconcile
```

### recon_config.yml

```yaml
version: 2
tables:
  - source_name: transactions
    target_name: transactions
    join_columns: [txn_id]
    filters:
      source: "txn_date >= DATEADD(day, -90, GETDATE())"
      target: "txn_date >= date_sub(current_date(), 90)"
    jdbc_reader_options:
      number_partitions: 50
      partition_column: txn_id
      lower_bound: "1"
      upper_bound: "10000000"
    transformations:
      - column_name: amount
        source: "coalesce(cast(amount as varchar), '_null_recon_')"
        target: "coalesce(cast(amount as string), '_null_recon_')"
```

---

## Example 4: Databricks-to-Databricks (Hive to Unity Catalog)

Validate migration from Hive metastore to Unity Catalog within the same workspace.

### Notebook

```python
from databricks.labs.lakebridge.config import (
    ReconcileConfig, DatabaseConfig, ReconcileMetadataConfig, TableRecon
)
from databricks.labs.lakebridge.reconcile.recon_config import Table
from databricks.labs.lakebridge.reconcile.trigger_recon_service import TriggerReconService
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()

# No secret_scope needed for Databricks-to-Databricks
reconcile_config = ReconcileConfig(
    data_source="databricks",
    report_type="all",
    secret_scope="",  # not needed
    database_config=DatabaseConfig(
        source_catalog="hive_metastore",
        source_schema="legacy_db",
        target_catalog="main",
        target_schema="migrated_db",
    ),
    metadata_config=ReconcileMetadataConfig(),
)

# Compare many tables at once
table_names = ["users", "products", "orders", "inventory", "shipments"]
table_recon = TableRecon(tables=[
    Table(source_name=t, target_name=t, join_columns=["id"])
    for t in table_names
])

result = TriggerReconService.trigger_recon(
    ws=ws, spark=spark, table_recon=table_recon, reconcile_config=reconcile_config
)
```

---

## Example 5: Aggregate-Only Validation

Verify aggregate metrics match without row-level comparison. Useful for very large tables where full comparison is too expensive.

### recon_config.yml

```yaml
version: 2
tables:
  - source_name: sales_fact
    target_name: sales_fact
    join_columns: [sale_id]
    aggregates:
      - type: SUM
        agg_columns: [revenue]
        group_by_columns: [region]
      - type: SUM
        agg_columns: [revenue]
        group_by_columns: [product_category, quarter]
      - type: COUNT
        agg_columns: [sale_id]
      - type: AVG
        agg_columns: [unit_price]
        group_by_columns: [product_category]
      - type: MIN
        agg_columns: [sale_date]
      - type: MAX
        agg_columns: [sale_date]
      - type: STDDEV
        agg_columns: [revenue]
        group_by_columns: [region]
```

### Run aggregate reconciliation

```bash
python -m databricks.labs.lakebridge.reconcile.execute aggregates-reconcile
```

Or in a notebook:

```python
from databricks.labs.lakebridge.reconcile.trigger_recon_aggregate_service import TriggerReconAggregateService

result = TriggerReconAggregateService.trigger_recon_aggregates(
    ws=ws, spark=spark, table_recon=table_recon, reconcile_config=reconcile_config
)
```
