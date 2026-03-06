[![CI](https://github.com/lbruand-db/mmf-dev-kit/actions/workflows/ci.yml/badge.svg)](https://github.com/lbruand-db/mmf-dev-kit/actions/workflows/ci.yml)

# Many-Model Forecasting (MMF) Dev Kit

A focused development kit for the **Many-Model Forecasting** skill, enabling AI coding assistants to build time series forecasting pipelines on Databricks.

## What's Included

| Component | Description |
|-----------|-------------|
| [`databricks-skills/many-model-forecasting/`](databricks-skills/many-model-forecasting/) | The MMF skill — patterns and best practices for forecasting on Databricks |
| [`.test/`](.test/) | Test infrastructure for evaluating the skill |

## The MMF Skill

The Many-Model Forecasting skill teaches AI assistants how to:

- Build forecasting pipelines using **MMF Solution Accelerator** (`mmf_sa`)
- Use statistical models (**StatsForecast**) and neural models (**NeuralForecast**)
- Run **Chronos** foundation models for zero-shot forecasting
- Orchestrate many-model training across Databricks clusters

## Prerequisites

This skill depends on the **Databricks MCP tools** (e.g., `connect_to_workspace`, `execute_parameterized_sql`) provided by [`ai-dev-kit`](https://github.com/databricks-solutions/ai-dev-kit). Make sure the Databricks MCP server from `ai-dev-kit` is configured in your AI coding tool before using the skill.

## Installing the Skill into Your Project

Clone this repo and run `install.py` to copy the skill files and configure your AI coding tools:

```bash
git clone <this-repo-url> mmf-dev-kit
cd mmf-dev-kit

# Install into your project (configures Claude Code, Cursor, and Gemini CLI)
python install.py --target /path/to/your-project

# Preview what will be created
python install.py --target /path/to/your-project --dry-run

# Configure only specific tools
python install.py --target /path/to/your-project --tools claude cursor
```

After installation your project will contain:

```
your-project/
├── CLAUDE.md                              # created or updated
├── GEMINI.md                              # created or updated
├── .cursor/rules/many-model-forecasting.mdc
└── databricks-skills/many-model-forecasting/
    ├── SKILL.md
    ├── 1-explore-data.md
    ├── 2-setup-the-mmf-cluster.md
    ├── 3-run-mmf.md
    ├── mmf_local_notebook_template.py
    └── mmf_gpu_notebook_template.py
```

Re-running the installer is safe — it updates existing configurations without duplication.

## Running Tests

From the `.test/` directory:

```bash
# Unit tests
uv run --extra dev python -m pytest tests/test_scorers.py -v

# Skill evaluation
uv run --extra dev python scripts/run_eval.py many-model-forecasting
```

## License

Copyright 2026 Databricks, Inc. Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0). See [LICENSE](LICENSE) for details.
