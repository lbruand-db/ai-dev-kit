# Lakebridge Reconcile — Secret Scope Setup

## Overview

Lakebridge uses Databricks secret scopes to store credentials for connecting to source systems. Each source type has a default scope name and expected secret keys.

## Creating a Secret Scope

```bash
# Create the scope
databricks secrets create-scope lakebridge_snowflake

# Add secrets
databricks secrets put-secret lakebridge_snowflake sfUrl --string-value "account.snowflakecomputing.com"
databricks secrets put-secret lakebridge_snowflake sfUser --string-value "migration_user"
databricks secrets put-secret lakebridge_snowflake sfPassword --string-value "your_password"
```

## Snowflake

**Default scope:** `lakebridge_snowflake`

| Secret key | Required | Description |
|-----------|----------|-------------|
| `sfUrl` | Yes | Snowflake account URL (e.g., `account.snowflakecomputing.com`) |
| `sfUser` | Yes | Snowflake username |
| `sfPassword` | Conditional | Password (used if PEM key not found) |
| `pem_private_key` | Conditional | Encrypted PEM private key (preferred over password) |
| `sfWarehouse` | No | Snowflake warehouse to use |
| `sfDatabase` | No | Snowflake database |
| `sfRole` | No | Snowflake role |

**Auth priority:** Lakebridge checks for `pem_private_key` first. If not found, falls back to `sfPassword`. If neither exists, an exception is raised.

```bash
# Password-based auth
databricks secrets create-scope lakebridge_snowflake
databricks secrets put-secret lakebridge_snowflake sfUrl --string-value "myaccount.snowflakecomputing.com"
databricks secrets put-secret lakebridge_snowflake sfUser --string-value "MIGRATION_SVC"
databricks secrets put-secret lakebridge_snowflake sfPassword --string-value "secure_password"
databricks secrets put-secret lakebridge_snowflake sfWarehouse --string-value "COMPUTE_WH"

# PEM key auth (preferred)
databricks secrets put-secret lakebridge_snowflake pem_private_key --string-value "$(cat /path/to/rsa_key.p8)"
```

## Oracle

**Default scope:** `lakebridge_oracle`

| Secret key | Required | Description |
|-----------|----------|-------------|
| `host` | Yes | Oracle hostname |
| `port` | Yes | Oracle port (typically `1521`) |
| `database` | Yes | Oracle SID or service name |
| `user` | Yes | Oracle username |
| `password` | Yes | Oracle password |

```bash
databricks secrets create-scope lakebridge_oracle
databricks secrets put-secret lakebridge_oracle host --string-value "oracle-host.example.com"
databricks secrets put-secret lakebridge_oracle port --string-value "1521"
databricks secrets put-secret lakebridge_oracle database --string-value "ORCL"
databricks secrets put-secret lakebridge_oracle user --string-value "migration_user"
databricks secrets put-secret lakebridge_oracle password --string-value "secure_password"
```

## MS SQL Server

**Default scope:** `lakebridge_mssql`

| Secret key | Required | Description |
|-----------|----------|-------------|
| `host` | Yes | SQL Server hostname |
| `port` | Yes | SQL Server port (typically `1433`) |
| `database` | Yes | Database name |
| `user` | Yes | SQL Server username |
| `password` | Yes | SQL Server password |

```bash
databricks secrets create-scope lakebridge_mssql
databricks secrets put-secret lakebridge_mssql host --string-value "sqlserver.example.com"
databricks secrets put-secret lakebridge_mssql port --string-value "1433"
databricks secrets put-secret lakebridge_mssql database --string-value "prod_db"
databricks secrets put-secret lakebridge_mssql user --string-value "migration_user"
databricks secrets put-secret lakebridge_mssql password --string-value "secure_password"
```

## Synapse

**Default scope:** `lakebridge_synapse`

| Secret key | Required | Description |
|-----------|----------|-------------|
| `host` | Yes | Synapse hostname |
| `port` | Yes | Synapse port (typically `1433`) |
| `database` | Yes | Database name |
| `user` | Yes | Synapse username |
| `password` | Yes | Synapse password |

```bash
databricks secrets create-scope lakebridge_synapse
databricks secrets put-secret lakebridge_synapse host --string-value "myworkspace.sql.azuresynapse.net"
databricks secrets put-secret lakebridge_synapse port --string-value "1433"
databricks secrets put-secret lakebridge_synapse database --string-value "prod_pool"
databricks secrets put-secret lakebridge_synapse user --string-value "migration_user"
databricks secrets put-secret lakebridge_synapse password --string-value "secure_password"
```

## Databricks (Source)

When both source and target are Databricks, **no secret scope is required**. Authentication uses the current workspace context.

```python
reconcile_config = ReconcileConfig(
    data_source="databricks",
    secret_scope="",  # empty — not needed
    ...
)
```

## Verifying Secrets

```bash
# List scopes
databricks secrets list-scopes

# List keys in a scope (values are hidden)
databricks secrets list-secrets lakebridge_snowflake
```

```python
# In a notebook — verify a secret is readable
dbutils.secrets.get(scope="lakebridge_snowflake", key="sfUrl")
```
