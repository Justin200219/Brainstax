# FTL Agent — Medallion Pipeline Agent

This repository contains a simple, table-driven Medallion (Bronze→Silver→Gold) ETL agent targeting Databricks.

Files of interest
- `agent/agent.py` — orchestration script; reads `configs/table_config.yaml`, runs operators and logs decisions to `dev.meta.agent_decisions`.
- `operators/bronze.py` — Bronze ingestion (idempotent MERGE into Delta)
- `operators/silver.py` — cleansing, DQ enforcement, PII hashing
- `operators/gold.py` — SCD2 dimension logic and fact passthrough
- `configs/table_config.yaml` — table metadata (sources, pk, dq rules, incremental hints)

Quick overview
- Designed to run on Databricks (notebook or Job). On Databricks a `spark` Session is provided automatically and Delta Lake is available.
- Can be run locally for development with `pyspark`/`delta-spark` installed or with Databricks Connect (recommended for exact parity with a Databricks cluster).

Prerequisites
- Databricks runtime with Delta (recommended when running in Databricks)
- For local development (optional): Python 3.8+ and packages: `pyspark`, `delta-spark`, `pyyaml`.

Local dev: quick setup (PowerShell)
```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install pyspark delta-spark pyyaml
```

Notes on running
- Databricks Repos: place this repo in Repos, open `agent/agent.py` in a notebook or run it as a Job entrypoint. Use relative config path (see below).
- Notebook: run cells or import `agent.agent` as needed; `spark` is available automatically.
- Job: point the Job to `agent/agent.py` in the repo; cluster must have access to source tables and write permissions to the `dev.*` catalog.

Config path
- `agent/agent.py` currently references a placeholder workspace path. Prefer using a repo-relative path. Example change in `agent.py`:
```python
import os
base_dir = os.path.dirname(__file__)
cfg_path = os.path.join(base_dir, "..", "configs", "table_config.yaml")
with open(cfg_path, "r") as f:
    config = yaml.safe_load(f)["tables"]
```

Recommended small edits
- Add a `SparkSession` guard when `spark` is not present so `agent.py` can run as a script locally.
- Quote SQL identifiers in `operators/*.py` when building MERGE/SET statements (use backticks) to avoid issues with special characters or reserved words.
- Add error handling around `spark.table(source)` to fail gracefully when source tables are missing.

Running the agent (databricks notebook)
1. Put the repo into Databricks Repos.
2. Open a notebook in the repo and run `%run ./agent/agent.py` or call functions from it.

Running locally (best-effort without Databricks Connect)
- The agent expects Delta functionality; local `delta-spark` provides this but behavior may differ from Databricks runtime. For exact parity use Databricks Connect.

Next steps (suggested)
- Add tests and a small sample dataset to validate the operators locally.
- Add CI/CD (GitHub Actions) to run static checks and unit tests.
- Optionally convert to Delta Live Tables (DLT) for a managed declarative pipeline.

Contact / support
- If you want, I can: (A) patch `agent.py` to add the SparkSession guard and repo-relative config path, (B) update `operators/*.py` to quote identifiers and add error handling, and (C) add a small test harness. Reply with which options to apply and I will implement them.
