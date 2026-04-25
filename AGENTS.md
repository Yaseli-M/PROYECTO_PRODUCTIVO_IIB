# AGENTS.md - Data Pipeline Project

## Overview
Data engineering pipeline for SIS health data (Medallion: Bronze → Silver → Gold) in PostgreSQL (Supabase).

## Setup
- **Venv**: `.SIS/` (activate with `source .SIS/bin/activate`).
- **Secrets**: Requires `.secrets/.toml` (not `.env`).
- **Directories**: Must exist: `data/raw/` and `data/processed/`.

## Pipeline Execution
1. **Extraction (Filter for 'CAJAMARCA')**:
   ```bash
   python scripts/orquestador.py
   ```
   *Interactive*: Prompts for years (e.g., `2017,2018`).
2. **Load to Bronze**:
   ```bash
   python scripts/carga_bronze.py
   ```
   *Interactive*: Prompts for file selection and user email.
   *Caution*: Database size limit (400MB). Uses `if_exists='append'` (not `replace`).
3. **Silver/Gold**: Run SP in SQL console:
   ```sql
   SELECT silver.sp_transform_bronze_to_silver();
   ```

## Development Notes
- **Imports**: Scripts use `sys.path.append(str(raiz))` to import from project root.
- **DB Connection**: Use `db_connection.py` for SQLAlchemy engines.
- **Logging**: Use `utils.logger.registrar_log` or `iniciar_proceso`/`finalizar_proceso`.
- **Naming**: Columns are lowercased and snake_cased automatically during `carga_bronze`.

## Git Exclusions
`.secrets/`, `data/`, `.SIS/`, `__pycache__/`
