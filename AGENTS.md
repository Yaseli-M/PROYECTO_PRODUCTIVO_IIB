# OpenCode Instructions (AGENTS.md)

## Architecture & Workflow
- **Data Engineering:** Uses Medallion Architecture (Bronze -> Silver -> Gold).
- **Processing:** Python (ETL) + PL/pgSQL (Warehouse transformations).
- **Database:** PostgreSQL (Supabase Cloud).
- **Incremental Loading:** Uses `meta.pipeline_config` with `last_watermark`.

## Key Commands & Procedures
- **Setup Environment:**
  ```bash
  mkdir -p data/raw data/processed .secrets
  python -m venv .SIS
  source .SIS/bin/activate
  pip install -r requirements.txt
  ```
- **Ingest Data:** `python scripts/carga_bronze.py` (Expects CSVs in `data/raw/`).
- **Transformations:** Triggered via SQL: `SELECT silver.sp_transform_bronze_to_silver();`.

## Gotchas
- **Secrets:** Keep all `.env` files inside `.secrets/` (Ignored by Git).
- **Environment:** The project assumes a virtual environment at `.SIS/`. Always ensure this is activated.
- **Workflow Order:** 
  1. Load raw data (`data/raw/`).
  2. Run `scripts/carga_bronze.py`.
  3. Execute DB-side transformations (Silver/Gold layers).
