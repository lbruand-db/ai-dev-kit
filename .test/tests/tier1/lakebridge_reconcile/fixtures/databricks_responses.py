"""Pre-canned Databricks responses for Tier 1 Lakebridge Reconcile mock tools.

Only contains responses for commands that cannot be executed on DuckDB:
- SHOW TABLES / SHOW SCHEMAS (DuckDB doesn't support Databricks syntax)
- Non-SQL tool responses (workspace connect, warehouses, secrets)

All other SQL responses are handled by the DuckDB backend.
"""

# ---- Source tables (simulating Snowflake source migrated to Databricks) ----
SHOW_TABLES_SOURCE = (
    "database,tableName,isTemporary\n"
    "public,customers,false\n"
    "public,orders,false\n"
    "public,order_items,false\n"
    "public,products,false"
)

# ---- Target tables (Unity Catalog after migration) ----
SHOW_TABLES_TARGET = (
    "database,tableName,isTemporary\n"
    "migrated,customers,false\n"
    "migrated,orders,false\n"
    "migrated,order_items,false\n"
    "migrated,products,false"
)

# ---- Reconciliation output tables ----
SHOW_TABLES_REMORPH = (
    "database,tableName,isTemporary\n"
    "reconcile,main_metrics,false\n"
    "reconcile,missing_in_src,false\n"
    "reconcile,missing_in_tgt,false\n"
    "reconcile,mismatch_data,false\n"
    "reconcile,schema_comparison,false\n"
    "reconcile,threshold_mismatch,false"
)

SHOW_SCHEMAS_SOURCE = (
    "databaseName\n"
    "public"
)

SHOW_SCHEMAS_TARGET = (
    "databaseName\n"
    "migrated"
)

SHOW_SCHEMAS_REMORPH = (
    "databaseName\n"
    "reconcile"
)

# ---- Secret scope fixtures ----
SECRET_SCOPES = {
    "lakebridge_snowflake": ["sfUrl", "sfUser", "sfPassword", "sfWarehouse"],
    "lakebridge_oracle": ["host", "port", "database", "user", "password"],
    "lakebridge_mssql": ["host", "port", "database", "user", "password"],
    "lakebridge_synapse": ["host", "port", "database", "user", "password"],
}

FIXTURES = {
    # SHOW TABLES for different catalogs/schemas
    "show_tables_source": SHOW_TABLES_SOURCE,
    "show_tables_target": SHOW_TABLES_TARGET,
    "show_tables_remorph": SHOW_TABLES_REMORPH,
    "show_schemas_source": SHOW_SCHEMAS_SOURCE,
    "show_schemas_target": SHOW_SCHEMAS_TARGET,
    "show_schemas_remorph": SHOW_SCHEMAS_REMORPH,
    # Non-SQL tool responses
    "workspace_connected": '{"status": "connected", "workspace": "test-workspace"}',
    "warehouses": (
        "id,name,state,cluster_size,max_num_clusters,auto_stop_mins\n"
        "mock-wh-001,Mock Warehouse,RUNNING,Small,1,120"
    ),
}
