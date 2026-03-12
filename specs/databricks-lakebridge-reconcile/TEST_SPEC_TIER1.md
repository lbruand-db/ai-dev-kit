# Tier 1 Skill Logic Tests — Lakebridge Reconcile

> **Status: DRAFT**

## Context

Tier 0 (the `.test/skills/databricks-lakebridge-reconcile/` ground truth tests) validates syntax, patterns, and expected facts in pre-canned responses — no LLM calls, no infrastructure. **Tier 1** validates that the skill, when loaded as a system prompt into an LLM, produces correct Lakebridge reconciliation configs and notebook code when given mock Databricks tool responses.

Unlike Tier 0, Tier 1 uses real LLM API calls (~$0.50–2/run) but **NO real Databricks infrastructure**. A DuckDB database acts as a mock target, simulating the source and target tables that the agent would query during reconciliation setup.

## Approach: Python + pytest + Databricks Foundation Model API + DuckDB + SQLGlot

Uses **Claude Sonnet on Databricks** (`databricks-claude-sonnet-4-6`) via the OpenAI-compatible Foundation Model API. A Python **tool-use agent loop** (`agent_runner.py`):

1. Sends SKILL.md content as system prompt + tool definitions to Claude Sonnet
2. Parses `tool_calls` from the response
3. Routes tool calls to mock handlers — SQL queries are **transpiled via SQLGlot** (Databricks → DuckDB) and executed on DuckDB
4. Feeds tool results back into the conversation
5. Repeats until the model stops calling tools

### Why this approach

- **No separate API key** — billed as DBUs on existing Databricks bill
- **All Python** — same ecosystem as Tier 0, same `uv run pytest` workflow
- **OpenAI-compatible API** — well-documented, stable, tool use fully supported
- **No MCP dependency** — mock tools are plain Python functions, simpler to debug
- **Real SQL execution** — SQLGlot transpiles Databricks SQL → DuckDB SQL, catches real query errors
- **Config validation** — generated YAML is parsed and validated against Lakebridge dataclasses

## What We're Testing

The skill generates two things: **config files** (reconcile.yml + recon_config.yml) and **notebook code**. Tier 1 tests that the agent:

1. Asks the right questions to gather source/target info
2. Generates valid `reconcile.yml` with correct `data_source`, `report_type`, `secret_scope`, `database_config`
3. Generates valid `recon_config.yml` with correct `tables`, `join_columns`, `column_mapping`, `transformations`, `thresholds`
4. Produces valid Python notebook code with correct imports and API calls
5. Chooses appropriate report types based on user requirements
6. Handles column name differences, type mismatches, and NULL handling in transformations
7. Correctly interprets mock reconciliation output tables

## File Structure

```
ai-dev-kit/
├── databricks-skills/
│   └── databricks-lakebridge-reconcile/
│       ├── SKILL.md                    # The skill under test
│       ├── configuration.md
│       ├── examples.md
│       └── secret_scopes.md
├── specs/
│   └── databricks-lakebridge-reconcile/
│       ├── SPEC.md                     # Skill specification
│       └── TEST_SPEC_TIER1.md          # This file
└── test/
    └── tier1/
        └── lakebridge-reconcile/
            ├── __init__.py
            ├── conftest.py             # Fixtures: OpenAI client, DuckDB conn, tool handlers
            ├── agent_runner.py         # Tool-use loop: prompt → tool_calls → mock → repeat
            ├── mock_tools.py           # Tool definitions + handler factory (bound to DuckDB)
            ├── duckdb_backend.py       # SQLGlot transpilation + DuckDB execution + seed data
            ├── config_validator.py     # Validates generated YAML against Lakebridge dataclasses
            ├── fixtures/
            │   ├── __init__.py
            │   └── databricks_responses.py  # Non-SQL fixtures (workspace, secrets, warehouses)
            ├── test_config_generation.py    # Tests 1–6: config generation
            ├── test_notebook_generation.py  # Tests 7–9: notebook code generation
            └── test_result_interpretation.py # Tests 10–12: interpreting reconciliation output
```

## DuckDB + SQLGlot Backend (`duckdb_backend.py`)

### Mock database design

An in-memory DuckDB database simulates **both the source system and the Databricks target** after migration. The key insight: we don't need to run actual Lakebridge reconciliation — we need to provide enough data context for the agent to generate correct configs.

#### Source tables (simulating the pre-migration source)

| Table | Rows | Description |
|-------|------|-------------|
| `source_db."public".customers` | 1,000 | Customer master data with deliberate column name differences |
| `source_db."public".orders` | 50,000 | Order fact table with high cardinality order_id |
| `source_db."public".order_items` | 150,000 | Line items with decimal precision differences |
| `source_db."public".products` | 500 | Product dimension table |

#### Target tables (simulating post-migration Unity Catalog)

| Table | Rows | Description |
|-------|------|-------------|
| `main."migrated".customers` | 998 | 2 rows missing (migration gap) |
| `main."migrated".orders` | 50,000 | Complete migration |
| `main."migrated".order_items` | 149,997 | 3 rows missing |
| `main."migrated".products` | 500 | Complete migration |

#### Deliberate data pathologies

These simulate real-world migration issues the agent should detect or account for:

| Issue | Location | Description |
|-------|----------|-------------|
| Missing rows | `main.migrated.customers` | 2 customers missing (id 500, 750) |
| Missing rows | `main.migrated.order_items` | 3 items missing |
| Column name mismatch | `source_db.public.customers.cust_name` | Target uses `customer_name` |
| Column name mismatch | `source_db.public.customers.cust_email` | Target uses `email` |
| Decimal precision | `source_db.public.order_items.unit_price` | Source: 4 decimals, target: 2 decimals |
| NULL values | `source_db.public.orders.discount` | 100 rows with NULL discount |
| Timestamp format | `source_db.public.customers.created_at` | Source: string `'2024-01-15'`, target: DATE type |
| Extra column | `source_db.public.products.internal_sku` | Column doesn't exist in target |

#### Reconciliation output tables (mock results)

To test result interpretation, pre-seed mock output tables:

| Table | Rows | Description |
|-------|------|-------------|
| `remorph."reconcile".main_metrics` | 4 | One row per table with match %, status |
| `remorph."reconcile".missing_in_src` | 0 | No rows missing from source |
| `remorph."reconcile".missing_in_tgt` | 5 | 2 customers + 3 order items |
| `remorph."reconcile".mismatch_data` | 12 | Rows with decimal precision mismatches |
| `remorph."reconcile".schema_comparison` | 20 | Column-by-column schema diff |

### SQL routing

```
SQL input (Databricks dialect)
  │
  ├── SHOW TABLES IN ...       → pre-canned fixture (DuckDB doesn't support SHOW TABLES)
  ├── SHOW SCHEMAS IN ...      → pre-canned fixture
  ├── DESCRIBE TABLE ...       → execute on DuckDB, remap columns to Databricks format
  ├── dbutils.secrets.*        → pre-canned fixture (scope exists, keys listed)
  └── Everything else          → sqlglot.transpile(read="databricks", write="duckdb") → execute
```

Key transpilations handled by SQLGlot:
- Three-part names: `catalog.schema.table` → DuckDB schema-qualified names
- `STRING` → `VARCHAR`
- `DESCRIBE TABLE x` → `DESCRIBE x` + output remapping
- Timestamp/date function differences

## Mock Tools (`mock_tools.py`)

Tool definitions in OpenAI function-calling format. Handlers created via `create_tool_handlers(conn)` factory bound to the DuckDB connection.

| Tool Name | Parameters | Behavior |
|-----------|-----------|----------|
| `execute_sql` | `statement` | SQLGlot transpile → DuckDB execute → CSV result |
| `list_warehouses` | — | Returns fixture: warehouse listing |
| `connect_to_workspace` | `workspace` | Returns fixture: `{"status": "connected"}` |
| `list_secrets` | `scope` | Returns fixture: key names for the given scope |
| `get_secret` | `scope`, `key` | Returns fixture: `"<REDACTED>"` (secrets are never shown) |

Only `execute_sql` hits DuckDB. All other tools return pre-canned fixtures.

## Config Validator (`config_validator.py`)

Parses YAML/Python output from the agent and validates against Lakebridge dataclasses:

```python
def validate_reconcile_yml(yaml_str: str) -> list[str]:
    """Parse and validate reconcile.yml content. Returns list of errors (empty = valid)."""
    # Check version == 1
    # Check data_source in {snowflake, oracle, mssql, synapse, databricks}
    # Check report_type in {schema, row, data, all}
    # Check secret_scope is non-empty (unless data_source == databricks)
    # Check database_config has required fields
    ...

def validate_recon_config_yml(yaml_str: str) -> list[str]:
    """Parse and validate recon_config.yml content. Returns list of errors (empty = valid)."""
    # Check version == 2
    # Check tables is a non-empty list
    # For each table: check source_name, target_name
    # If report_type is data/all: check join_columns present
    # Validate column_thresholds.type against SQLGLOT types
    # Validate table_thresholds bounds and model
    # Validate aggregate types
    ...

def validate_notebook_code(code_str: str) -> list[str]:
    """Parse and validate generated notebook code. Returns list of errors (empty = valid)."""
    # Check syntax validity (ast.parse)
    # Check imports from correct modules
    # Check ReconcileConfig and TableRecon are constructed
    # Check TriggerReconService.trigger_recon() is called
    ...
```

## Test Cases (12 tests)

### Config Generation (6 tests) — `test_config_generation.py`

**Test 1: Basic Snowflake reconciliation config**
- User prompt: "I migrated tables from Snowflake (PROD_DB.PUBLIC) to Databricks (main.migrated). Generate reconcile configs to validate the migration. Tables: customers, orders, order_items, products."
- Assert: `reconcile.yml` has `data_source: snowflake`, `report_type: all`, `secret_scope: lakebridge_snowflake`
- Assert: `recon_config.yml` has 4 tables with correct source/target names
- Assert: Both configs pass `config_validator`

**Test 2: Column mapping detection**
- User prompt: "The source table customers has columns cust_name and cust_email, but the target uses customer_name and email. Generate the recon config."
- Assert: `recon_config.yml` includes `column_mapping` entries for both columns
- Assert: `join_columns` uses source column name

**Test 3: Threshold configuration for decimal columns**
- User prompt: "The order_items.unit_price column may have small precision differences after migration. Configure thresholds to allow up to 0.01 difference."
- Assert: `column_thresholds` with `lower_bound: "-0.01"`, `upper_bound: "0.01"`, `type: decimal`
- Assert: `transformations` include NULL handling with `coalesce`

**Test 4: Schema-only report type selection**
- User prompt: "I just want a quick schema check before running full data comparison."
- Assert: `report_type: schema`
- Assert: Tables do NOT require `join_columns`

**Test 5: Filtered reconciliation**
- User prompt: "Only reconcile orders from the last 90 days. Source is SQL Server."
- Assert: `filters.source` uses MSSQL dialect (`DATEADD`, `GETDATE`)
- Assert: `filters.target` uses Databricks dialect (`date_sub`, `current_date`)
- Assert: `data_source: mssql`, `secret_scope: lakebridge_mssql`

**Test 6: Aggregate validation config**
- User prompt: "Generate config to verify SUM(revenue), COUNT(order_id), and AVG(unit_price) by region match between source and target."
- Assert: `aggregates` list with correct `type`, `agg_columns`, `group_by_columns`
- Assert: At least 3 aggregates defined

### Notebook Generation (3 tests) — `test_notebook_generation.py`

**Test 7: Valid notebook code for Snowflake source**
- User prompt: "Generate a Python notebook that runs Lakebridge reconciliation for Snowflake source to main.migrated."
- Assert: Code passes `ast.parse()` (valid Python syntax)
- Assert: Imports from `databricks.labs.lakebridge.config` (not `recon_config`)
- Assert: `ReconcileConfig`, `TableRecon`, and `TriggerReconService.trigger_recon()` present
- Assert: Code passes `validate_notebook_code()`

**Test 8: Notebook with JDBC reader options for large tables**
- User prompt: "Generate notebook code for reconciling a 100M-row orders table from Oracle. Include JDBC reader options."
- Assert: `JdbcReaderOptions` with `number_partitions`, `partition_column`, `lower_bound`, `upper_bound`
- Assert: `fetch_size` > 100 (tuned for large table)

**Test 9: Databricks-to-Databricks notebook**
- User prompt: "Generate notebook to validate Hive metastore migration to Unity Catalog. Source: hive_metastore.legacy_db, target: main.migrated_db."
- Assert: `data_source="databricks"`
- Assert: `secret_scope` is empty string
- Assert: `source_catalog="hive_metastore"`

### Result Interpretation (3 tests) — `test_result_interpretation.py`

**Test 10: Interpret overall reconciliation status**
- User prompt: "My reconciliation ran with recon_id='abc123'. What's the overall status?"
- Mock: Agent queries `remorph.reconcile.main_metrics` → DuckDB returns pre-seeded rows
- Assert: Agent reports per-table pass/fail status
- Assert: Agent mentions tables with issues (customers, order_items)

**Test 11: Diagnose missing rows**
- User prompt: "Recon shows 2 customers missing in target. What happened and how do I fix it?"
- Mock: Agent queries `remorph.reconcile.missing_in_tgt` → DuckDB returns 2 rows
- Assert: Agent suggests checking migration logs or re-running migration for those rows
- Assert: Agent references specific customer IDs from the mock data

**Test 12: Diagnose column mismatches and suggest fixes**
- User prompt: "I'm seeing mismatch_data for order_items. The unit_price values are slightly different."
- Mock: Agent queries `remorph.reconcile.mismatch_data` → DuckDB returns rows with decimal diffs
- Assert: Agent suggests adding `column_thresholds` or `transformations`
- Assert: Agent provides specific config snippet with appropriate bounds

## Agent Runner (`agent_runner.py`)

Multi-turn tool-use loop:

- **Model**: `databricks-claude-sonnet-4-6` via OpenAI-compatible API
- **Temperature**: 0 (deterministic for reproducibility)
- **Max turns**: 20 (configurable per test)
- **System prompt**: Concatenation of SKILL.md + configuration.md + examples.md + secret_scopes.md
- **Empty content fix**: When assistant messages have `tool_calls` but no text content, omit the `content` key (Databricks FMAPI returns 400 if `content: ""`)

Returns: `{"messages": [...], "tool_calls": [...], "final_response": str, "turns": int}`

## Dependencies

```toml
# pyproject.toml additions
[project.optional-dependencies]
tier1-reconcile = [
    "duckdb>=1.0",
    "sqlglot>=28.0",
    "openai",
    "pyyaml>=6.0",
    "pytz",
    "pytest>=8.0",
    "python-dotenv>=1.0.0",
]

[tool.pytest.ini_options]
markers = [
    "tier0: Static and unit tests (no infra needed)",
    "tier1: Skill logic tests (mock tools, requires Databricks Claude Sonnet)",
]
```

## Running

```bash
# Install deps
uv sync --extra tier1-reconcile

# Run Tier 1 tests (requires DATABRICKS_TOKEN)
DATABRICKS_TOKEN=$(databricks auth token) \
  uv run pytest test/tier1/lakebridge-reconcile/ -v -m tier1

# Or with .env file (auto-loaded by python-dotenv in conftest.py)
uv run pytest test/tier1/lakebridge-reconcile/ -v -m tier1

# Run a single test
uv run pytest test/tier1/lakebridge-reconcile/test_config_generation.py::test_basic_snowflake_config -v -m tier1
```

## Environment Requirements

- **`.env` file** at project root with `DATABRICKS_TOKEN` (PAT) — git-ignored
- `DATABRICKS_HOST` defaults to workspace URL (configurable)
- Python >= 3.10
- No Node.js, no Anthropic API key
- `python-dotenv` loads `.env` automatically in conftest

## Cost & Risk

| Factor | Estimate |
|--------|----------|
| Cost per run | ~$1–3 (12 tests × ~$0.10–0.25 each), billed as DBUs |
| Pricing | ~$3/1M input tokens, ~$15/1M output tokens (Sonnet) |
| Runtime | ~5–10 min (12 tests, 20 max turns each) |

| Risk | Mitigation |
|------|------------|
| LLM non-determinism | Assert on patterns/structure, not exact strings; `temperature=0` |
| Tool call format differences | OpenAI-compatible format well-tested on Databricks |
| SQL transpilation gaps | SQLGlot handles most Databricks→DuckDB; `SHOW TABLES` needs fixture fallback |
| YAML generation variation | Validate via `config_validator.py` dataclass parsing, not string matching |
| Config field ordering | Parse YAML to dict, compare semantically |

## Key Design Decisions

1. **Mock at the tool level, not at Lakebridge library level.** We're testing the *skill* (config/code generation), not the Lakebridge reconciliation engine itself. The agent calls `execute_sql` to explore tables and generates configs — we mock those SQL queries via DuckDB.

2. **Pre-seed reconciliation output tables.** Tests 10–12 need the agent to interpret results. Rather than running actual Lakebridge, we seed `remorph.reconcile.*` tables in DuckDB with known pathologies.

3. **Validate configs structurally, not textually.** The agent may produce valid YAML in different orderings or styles. `config_validator.py` parses YAML into Lakebridge dataclasses and checks field values.

4. **Separate source and target schemas in DuckDB.** Using `source_db.public.*` and `main.migrated.*` schemas mirrors real three-part naming and tests that the agent generates correct `database_config`.
