"""DuckDB backend for Tier 1 Lakebridge Reconcile tests.

Seeds an in-memory DuckDB database with:
- Source tables (source_db.public.*) simulating pre-migration data
- Target tables (uc_main.migrated.*) simulating post-migration Unity Catalog
- Reconciliation output tables (remorph.reconcile.*) for result interpretation tests

Note: DuckDB reserves "main" as a database name. We use "uc_main" internally
and remap "main." -> "uc_main." in SQL before execution.

Transpiles Databricks SQL to DuckDB SQL via SQLGlot and executes queries.
"""

from __future__ import annotations

import re

import duckdb
import sqlglot

from test.tier1.lakebridge_reconcile.fixtures.databricks_responses import (
    FIXTURES,
)

# ---------------------------------------------------------------------------
# Type mapping: DuckDB types back to Databricks display types
# ---------------------------------------------------------------------------

_DUCKDB_TO_DATABRICKS_TYPE = {
    "VARCHAR": "STRING",
    "TEXT": "STRING",
    "BIGINT": "BIGINT",
    "INTEGER": "INT",
    "INT": "INT",
    "SMALLINT": "SMALLINT",
    "TINYINT": "TINYINT",
    "DOUBLE": "DOUBLE",
    "FLOAT": "FLOAT",
    "REAL": "FLOAT",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE": "TIMESTAMP",
    "TIMESTAMPTZ": "TIMESTAMP",
}


def _to_databricks_type(duckdb_type: str) -> str:
    """Map a DuckDB column type string to its Databricks equivalent."""
    upper = duckdb_type.upper()
    base = upper.split("(")[0].strip()
    if base in ("DECIMAL", "NUMERIC"):
        return upper
    return _DUCKDB_TO_DATABRICKS_TYPE.get(upper, upper)


# ---------------------------------------------------------------------------
# Catalog name remapping
# ---------------------------------------------------------------------------

# DuckDB reserves "main" — we store target tables under "uc_main" and remap.
_CATALOG_REMAP = {
    "main.": "uc_main.",
}


def _remap_catalogs(sql: str) -> str:
    """Replace Databricks catalog names with DuckDB-safe names."""
    result = sql
    for src, dst in _CATALOG_REMAP.items():
        # Case-insensitive replacement for catalog.schema references
        result = re.sub(re.escape(src), dst, result, flags=re.IGNORECASE)
    return result


# ---------------------------------------------------------------------------
# Database seeding
# ---------------------------------------------------------------------------


def create_test_database() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB database seeded with Lakebridge reconcile test data.

    Source tables (source_db.public.*):
    - customers: 1,000 rows with column name differences (cust_name, cust_email)
    - orders: 5,000 rows with NULLable discount column
    - order_items: 15,000 rows with decimal precision differences
    - products: 500 rows with extra internal_sku column

    Target tables (uc_main.migrated.* — remapped from main.migrated.*):
    - customers: 998 rows (2 missing: id 500, 750)
    - orders: 5,000 rows (complete)
    - order_items: 14,997 rows (3 missing)
    - products: 500 rows (no internal_sku column)

    Reconciliation output tables (remorph.reconcile.*):
    - main_metrics: 4 rows (one per table)
    - missing_in_src: 0 rows
    - missing_in_tgt: 5 rows (2 customers + 3 order_items)
    - mismatch_data: 12 rows (decimal precision mismatches)
    - schema_comparison: 20 rows (column-by-column schema diff)

    Note: Row counts reduced from spec (1K/5K/15K vs 1K/50K/150K) for speed.
    """
    conn = duckdb.connect(":memory:")

    _create_source_tables(conn)
    _create_target_tables(conn)
    _create_recon_output_tables(conn)

    return conn


def _create_source_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create source tables simulating a pre-migration Snowflake database."""
    conn.execute("ATTACH ':memory:' AS source_db")
    conn.execute('CREATE SCHEMA source_db."public"')

    # --- customers: 1,000 rows ---
    conn.execute("""
        CREATE TABLE source_db."public".customers (
            customer_id INTEGER,
            cust_name VARCHAR,
            cust_email VARCHAR,
            credit_limit DECIMAL(15, 4),
            created_at VARCHAR,
            region VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO source_db."public".customers
        SELECT
            c AS customer_id,
            'Customer ' || CAST(c AS VARCHAR) AS cust_name,
            'cust' || CAST(c AS VARCHAR) || '@example.com' AS cust_email,
            ROUND(CAST((c * 17 % 10000) + (c * 3 % 100) / 100.0 AS DECIMAL(15, 4)), 4) AS credit_limit,
            CAST(DATE '2020-01-01' + INTERVAL (c % 1000) DAY AS VARCHAR) AS created_at,
            CASE WHEN c % 4 = 0 THEN 'north'
                 WHEN c % 4 = 1 THEN 'south'
                 WHEN c % 4 = 2 THEN 'east'
                 ELSE 'west' END AS region
        FROM generate_series(1, 1000) AS t(c)
    """)

    # --- orders: 5,000 rows (100 with NULL discount) ---
    conn.execute("""
        CREATE TABLE source_db."public".orders (
            order_id INTEGER,
            customer_id INTEGER,
            order_date DATE,
            total_amount DOUBLE,
            discount DOUBLE,
            status VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO source_db."public".orders
        SELECT
            o AS order_id,
            (o % 1000) + 1 AS customer_id,
            DATE '2023-01-01' + INTERVAL (o % 365) DAY AS order_date,
            ROUND((o * 7 % 10000) / 10.0, 2) AS total_amount,
            CASE WHEN o <= 100 THEN NULL
                 ELSE ROUND((o % 30) / 100.0, 2) END AS discount,
            CASE WHEN o % 5 = 0 THEN 'shipped'
                 WHEN o % 5 = 1 THEN 'delivered'
                 WHEN o % 5 = 2 THEN 'pending'
                 WHEN o % 5 = 3 THEN 'cancelled'
                 ELSE 'processing' END AS status
        FROM generate_series(1, 5000) AS t(o)
    """)

    # --- order_items: 15,000 rows (unit_price with 4 decimal places) ---
    conn.execute("""
        CREATE TABLE source_db."public".order_items (
            order_id INTEGER,
            item_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            unit_price DECIMAL(15, 4)
        )
    """)
    conn.execute("""
        INSERT INTO source_db."public".order_items
        SELECT
            ((i - 1) / 3) + 1 AS order_id,
            ((i - 1) % 3) + 1 AS item_id,
            (i % 500) + 1 AS product_id,
            (i % 10) + 1 AS quantity,
            ROUND(CAST((i * 13 % 10000) / 100.0 AS DECIMAL(15, 4)), 4) AS unit_price
        FROM generate_series(1, 15000) AS t(i)
    """)

    # --- products: 500 rows (has internal_sku, target does NOT) ---
    conn.execute("""
        CREATE TABLE source_db."public".products (
            product_id INTEGER,
            product_name VARCHAR,
            category VARCHAR,
            price DOUBLE,
            internal_sku VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO source_db."public".products
        SELECT
            p AS product_id,
            'Product ' || CAST(p AS VARCHAR) AS product_name,
            CASE WHEN p % 5 = 0 THEN 'electronics'
                 WHEN p % 5 = 1 THEN 'clothing'
                 WHEN p % 5 = 2 THEN 'food'
                 WHEN p % 5 = 3 THEN 'furniture'
                 ELSE 'toys' END AS category,
            ROUND((p * 11 % 10000) / 100.0, 2) AS price,
            'SKU-' || LPAD(CAST(p AS VARCHAR), 5, '0') AS internal_sku
        FROM generate_series(1, 500) AS t(p)
    """)


def _create_target_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create target tables simulating post-migration Unity Catalog.

    Uses 'uc_main' as the DuckDB database name since 'main' is reserved.
    The SQL execution layer remaps 'main.' -> 'uc_main.' transparently.
    """
    conn.execute("ATTACH ':memory:' AS uc_main")
    conn.execute('CREATE SCHEMA uc_main."migrated"')

    # --- customers: 998 rows (missing id 500, 750) ---
    conn.execute("""
        CREATE TABLE uc_main."migrated".customers (
            customer_id INTEGER,
            customer_name VARCHAR,
            email VARCHAR,
            credit_limit DECIMAL(15, 2),
            created_at DATE,
            region VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO uc_main."migrated".customers
        SELECT
            c AS customer_id,
            'Customer ' || CAST(c AS VARCHAR) AS customer_name,
            'cust' || CAST(c AS VARCHAR) || '@example.com' AS email,
            ROUND(CAST((c * 17 % 10000) + (c * 3 % 100) / 100.0 AS DECIMAL(15, 2)), 2) AS credit_limit,
            CAST(DATE '2020-01-01' + INTERVAL (c % 1000) DAY AS DATE) AS created_at,
            CASE WHEN c % 4 = 0 THEN 'north'
                 WHEN c % 4 = 1 THEN 'south'
                 WHEN c % 4 = 2 THEN 'east'
                 ELSE 'west' END AS region
        FROM generate_series(1, 1000) AS t(c)
        WHERE c NOT IN (500, 750)
    """)

    # --- orders: 5,000 rows (complete migration) ---
    conn.execute("""
        CREATE TABLE uc_main."migrated".orders (
            order_id INTEGER,
            customer_id INTEGER,
            order_date DATE,
            total_amount DOUBLE,
            discount DOUBLE,
            status VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO uc_main."migrated".orders
        SELECT
            o AS order_id,
            (o % 1000) + 1 AS customer_id,
            DATE '2023-01-01' + INTERVAL (o % 365) DAY AS order_date,
            ROUND((o * 7 % 10000) / 10.0, 2) AS total_amount,
            CASE WHEN o <= 100 THEN NULL
                 ELSE ROUND((o % 30) / 100.0, 2) END AS discount,
            CASE WHEN o % 5 = 0 THEN 'shipped'
                 WHEN o % 5 = 1 THEN 'delivered'
                 WHEN o % 5 = 2 THEN 'pending'
                 WHEN o % 5 = 3 THEN 'cancelled'
                 ELSE 'processing' END AS status
        FROM generate_series(1, 5000) AS t(o)
    """)

    # --- order_items: 14,997 rows (3 missing: items 7500, 10000, 12500) ---
    conn.execute("""
        CREATE TABLE uc_main."migrated".order_items (
            order_id INTEGER,
            item_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            unit_price DECIMAL(15, 2)
        )
    """)
    conn.execute("""
        INSERT INTO uc_main."migrated".order_items
        SELECT
            ((i - 1) / 3) + 1 AS order_id,
            ((i - 1) % 3) + 1 AS item_id,
            (i % 500) + 1 AS product_id,
            (i % 10) + 1 AS quantity,
            ROUND(CAST((i * 13 % 10000) / 100.0 AS DECIMAL(15, 2)), 2) AS unit_price
        FROM generate_series(1, 15000) AS t(i)
        WHERE i NOT IN (7500, 10000, 12500)
    """)

    # --- products: 500 rows (no internal_sku column) ---
    conn.execute("""
        CREATE TABLE uc_main."migrated".products (
            product_id INTEGER,
            product_name VARCHAR,
            category VARCHAR,
            price DOUBLE
        )
    """)
    conn.execute("""
        INSERT INTO uc_main."migrated".products
        SELECT
            p AS product_id,
            'Product ' || CAST(p AS VARCHAR) AS product_name,
            CASE WHEN p % 5 = 0 THEN 'electronics'
                 WHEN p % 5 = 1 THEN 'clothing'
                 WHEN p % 5 = 2 THEN 'food'
                 WHEN p % 5 = 3 THEN 'furniture'
                 ELSE 'toys' END AS category,
            ROUND((p * 11 % 10000) / 100.0, 2) AS price
        FROM generate_series(1, 500) AS t(p)
    """)


def _create_recon_output_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create pre-seeded reconciliation output tables for result interpretation tests."""
    conn.execute("ATTACH ':memory:' AS remorph")
    conn.execute('CREATE SCHEMA remorph."reconcile"')

    # --- main_metrics: one row per table ---
    conn.execute("""
        CREATE TABLE remorph."reconcile".main_metrics (
            recon_id VARCHAR,
            source_table VARCHAR,
            target_table VARCHAR,
            source_row_count BIGINT,
            target_row_count BIGINT,
            missing_in_src BIGINT,
            missing_in_tgt BIGINT,
            mismatch_count BIGINT,
            match_percentage DOUBLE,
            status VARCHAR,
            recon_type VARCHAR,
            start_ts TIMESTAMP,
            end_ts TIMESTAMP
        )
    """)
    conn.execute("""
        INSERT INTO remorph."reconcile".main_metrics VALUES
        ('abc123', 'source_db.public.customers', 'main.migrated.customers',
         1000, 998, 0, 2, 0, 99.8, 'FAILED', 'all',
         TIMESTAMP '2024-03-01 10:00:00', TIMESTAMP '2024-03-01 10:05:00'),
        ('abc123', 'source_db.public.orders', 'main.migrated.orders',
         5000, 5000, 0, 0, 0, 100.0, 'PASSED', 'all',
         TIMESTAMP '2024-03-01 10:05:00', TIMESTAMP '2024-03-01 10:15:00'),
        ('abc123', 'source_db.public.order_items', 'main.migrated.order_items',
         15000, 14997, 0, 3, 12, 99.9, 'FAILED', 'all',
         TIMESTAMP '2024-03-01 10:15:00', TIMESTAMP '2024-03-01 10:30:00'),
        ('abc123', 'source_db.public.products', 'main.migrated.products',
         500, 500, 0, 0, 0, 100.0, 'PASSED', 'all',
         TIMESTAMP '2024-03-01 10:30:00', TIMESTAMP '2024-03-01 10:32:00')
    """)

    # --- missing_in_src: 0 rows ---
    conn.execute("""
        CREATE TABLE remorph."reconcile".missing_in_src (
            recon_id VARCHAR,
            source_table VARCHAR,
            target_table VARCHAR,
            missing_row VARCHAR
        )
    """)

    # --- missing_in_tgt: 5 rows (2 customers + 3 order_items) ---
    conn.execute("""
        CREATE TABLE remorph."reconcile".missing_in_tgt (
            recon_id VARCHAR,
            source_table VARCHAR,
            target_table VARCHAR,
            missing_row VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO remorph."reconcile".missing_in_tgt VALUES
        ('abc123', 'source_db.public.customers', 'main.migrated.customers',
         '{"customer_id": 500, "cust_name": "Customer 500"}'),
        ('abc123', 'source_db.public.customers', 'main.migrated.customers',
         '{"customer_id": 750, "cust_name": "Customer 750"}'),
        ('abc123', 'source_db.public.order_items', 'main.migrated.order_items',
         '{"order_id": 2500, "item_id": 1}'),
        ('abc123', 'source_db.public.order_items', 'main.migrated.order_items',
         '{"order_id": 3334, "item_id": 1}'),
        ('abc123', 'source_db.public.order_items', 'main.migrated.order_items',
         '{"order_id": 4167, "item_id": 1}')
    """)

    # --- mismatch_data: 12 rows of decimal precision mismatches ---
    conn.execute("""
        CREATE TABLE remorph."reconcile".mismatch_data (
            recon_id VARCHAR,
            source_table VARCHAR,
            target_table VARCHAR,
            column_name VARCHAR,
            source_value VARCHAR,
            target_value VARCHAR,
            join_key VARCHAR
        )
    """)
    mismatch_rows = []
    for i in range(12):
        src_val = f"{10.0 + i * 0.1234:.4f}"
        tgt_val = f"{10.0 + i * 0.1234:.2f}"
        join_key = f'{{"order_id": {1000 + i}, "item_id": 1}}'
        mismatch_rows.append(
            f"('abc123', 'source_db.public.order_items', 'main.migrated.order_items',"
            f" 'unit_price', '{src_val}', '{tgt_val}', '{join_key}')"
        )
    conn.execute(
        f'INSERT INTO remorph."reconcile".mismatch_data VALUES {",".join(mismatch_rows)}'
    )

    # --- schema_comparison: 20 rows (column-by-column diff) ---
    conn.execute("""
        CREATE TABLE remorph."reconcile".schema_comparison (
            recon_id VARCHAR,
            source_table VARCHAR,
            target_table VARCHAR,
            source_column VARCHAR,
            target_column VARCHAR,
            source_datatype VARCHAR,
            target_datatype VARCHAR,
            is_valid BOOLEAN
        )
    """)
    schema_rows = [
        ("customers", "customer_id", "customer_id", "NUMBER(38,0)", "INT", True),
        ("customers", "cust_name", "customer_name", "VARCHAR(255)", "STRING", True),
        ("customers", "cust_email", "email", "VARCHAR(255)", "STRING", True),
        ("customers", "credit_limit", "credit_limit", "NUMBER(15,4)", "DECIMAL(15,2)", False),
        ("customers", "created_at", "created_at", "TIMESTAMP_NTZ", "DATE", False),
        ("customers", "region", "region", "VARCHAR(50)", "STRING", True),
        ("orders", "order_id", "order_id", "NUMBER(38,0)", "INT", True),
        ("orders", "customer_id", "customer_id", "NUMBER(38,0)", "INT", True),
        ("orders", "order_date", "order_date", "DATE", "DATE", True),
        ("orders", "total_amount", "total_amount", "FLOAT", "DOUBLE", True),
        ("orders", "discount", "discount", "FLOAT", "DOUBLE", True),
        ("orders", "status", "status", "VARCHAR(50)", "STRING", True),
        ("order_items", "order_id", "order_id", "NUMBER(38,0)", "INT", True),
        ("order_items", "item_id", "item_id", "NUMBER(38,0)", "INT", True),
        ("order_items", "product_id", "product_id", "NUMBER(38,0)", "INT", True),
        ("order_items", "quantity", "quantity", "NUMBER(38,0)", "INT", True),
        ("order_items", "unit_price", "unit_price", "NUMBER(15,4)", "DECIMAL(15,2)", False),
        ("products", "product_id", "product_id", "NUMBER(38,0)", "INT", True),
        ("products", "product_name", "product_name", "VARCHAR(255)", "STRING", True),
        ("products", "category", "category", "VARCHAR(50)", "STRING", True),
    ]
    values = []
    for table, src_col, tgt_col, src_type, tgt_type, valid in schema_rows:
        values.append(
            f"('abc123', 'source_db.public.{table}', 'main.migrated.{table}',"
            f" '{src_col}', '{tgt_col}', '{src_type}', '{tgt_type}', {str(valid).lower()})"
        )
    conn.execute(
        f'INSERT INTO remorph."reconcile".schema_comparison VALUES {",".join(values)}'
    )


# ---------------------------------------------------------------------------
# SQL transpilation and execution
# ---------------------------------------------------------------------------


def transpile_sql(databricks_sql: str) -> str:
    """Transpile a Databricks SQL statement to DuckDB dialect via SQLGlot."""
    try:
        results = sqlglot.transpile(databricks_sql, read="databricks", write="duckdb")
        return results[0]
    except sqlglot.errors.ParseError:
        return databricks_sql


def _format_as_csv(description: list, rows: list) -> str:
    """Format DuckDB query results as CSV string (Databricks SQL output format)."""
    col_names = [col[0] for col in description]
    lines = [",".join(col_names)]
    for row in rows:
        lines.append(",".join(_format_value(v) for v in row))
    return "\n".join(lines)


def _format_value(v: object) -> str:
    """Format a single value for CSV output, matching Databricks formatting."""
    if v is None:
        return "null"
    if isinstance(v, bool):
        return str(v).lower()
    if isinstance(v, float):
        if v == int(v) and abs(v) < 1e15:
            return str(int(v))
        return f"{v:g}"
    s = str(v)
    if " 00:00:00" in s:
        s = s.replace(" 00:00:00", "")
    s = re.sub(r"[+-]\d{2}:\d{2}$", "", s)
    return s


def _is_show_tables(sql: str) -> bool:
    return bool(re.match(r"\s*SHOW\s+TABLES\s", sql, re.IGNORECASE))


def _is_show_schemas(sql: str) -> bool:
    return bool(re.match(r"\s*SHOW\s+(SCHEMAS|DATABASES)\s", sql, re.IGNORECASE))


def _is_describe(sql: str) -> bool:
    return bool(re.match(r"\s*DESCRIBE\s", sql, re.IGNORECASE))


def _route_show_tables(sql: str) -> str:
    """Route SHOW TABLES to the correct fixture based on catalog/schema reference."""
    sql_upper = sql.upper()
    if "REMORPH" in sql_upper or "RECONCILE" in sql_upper:
        return FIXTURES["show_tables_remorph"]
    if "MAIN" in sql_upper or "MIGRATED" in sql_upper:
        return FIXTURES["show_tables_target"]
    return FIXTURES["show_tables_source"]


def _route_show_schemas(sql: str) -> str:
    """Route SHOW SCHEMAS to the correct fixture based on catalog reference."""
    sql_upper = sql.upper()
    if "REMORPH" in sql_upper:
        return FIXTURES["show_schemas_remorph"]
    if "MAIN" in sql_upper:
        return FIXTURES["show_schemas_target"]
    return FIXTURES["show_schemas_source"]


def execute_sql(conn: duckdb.DuckDBPyConnection, databricks_sql: str) -> str:
    """Execute a Databricks SQL statement against DuckDB.

    - SHOW TABLES: routes to pre-canned fixture based on catalog/schema
    - SHOW SCHEMAS: routes to pre-canned fixture based on catalog
    - DESCRIBE: executes on DuckDB, remaps output to Databricks column format
    - Everything else: transpiles via SQLGlot, remaps catalogs, and executes
    """
    sql = databricks_sql.strip()

    if _is_show_tables(sql):
        return _route_show_tables(sql)

    if _is_show_schemas(sql):
        return _route_show_schemas(sql)

    if _is_describe(sql):
        return _handle_describe(conn, sql)

    # Transpile Databricks SQL -> DuckDB SQL, then remap catalog names
    duckdb_sql = _remap_catalogs(transpile_sql(sql))
    try:
        result = conn.execute(duckdb_sql)
    except duckdb.Error as e:
        return f"error\n{e}"
    if result.description:
        rows = result.fetchall()
        return _format_as_csv(result.description, rows)
    return "status\nSUCCEEDED"


def _handle_describe(conn: duckdb.DuckDBPyConnection, sql: str) -> str:
    """Execute DESCRIBE on DuckDB and remap output to Databricks format.

    DuckDB DESCRIBE returns: column_name, column_type, null, key, default, extra
    Databricks returns: col_name, data_type, comment
    """
    # First remap catalogs (main. -> uc_main.), then transpile once.
    remapped_sql = _remap_catalogs(sql)
    duckdb_sql = transpile_sql(remapped_sql)

    try:
        result = conn.execute(duckdb_sql)
    except duckdb.Error:
        # Fallback: extract table ref and try DESCRIBE directly
        table_match = re.search(
            r"DESCRIBE\s+(?:TABLE\s+)?(.+)", remapped_sql, re.IGNORECASE
        )
        if table_match:
            table_ref = table_match.group(1).strip().rstrip(";")
            try:
                result = conn.execute(f"DESCRIBE {table_ref}")
            except duckdb.Error as e:
                return f"error\n{e}"
        else:
            return "error\nCannot parse DESCRIBE statement"

    rows = result.fetchall()
    lines = ["col_name,data_type,comment"]
    for row in rows:
        col_name = row[0]
        col_type = _to_databricks_type(str(row[1]))
        lines.append(f"{col_name},{col_type},")
    return "\n".join(lines)
