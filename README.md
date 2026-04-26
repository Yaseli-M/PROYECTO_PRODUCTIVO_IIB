# Pipeline SIS - Arquitectura Medallion (Bronze -> Silver -> Gold)

Este repositorio implementa un flujo de ingenieria de datos para procesar atenciones SIS,
desde archivos CSV crudos hasta una capa analitica en PostgreSQL/Supabase.

El proceso operativo en este repo es:

1. **Extraer y filtrar** registros de Cajamarca desde `data/raw` hacia `data/processed`.
2. **Cargar Bronze** (`bronze.atenciones_sis_raw`) desde los CSV procesados.
3. **Transformar a Silver/Gold** mediante procedimientos SQL en la base de datos.

---

## Arquitectura

### Bronze (Raw)

- Tabla: `bronze.atenciones_sis_raw`
- Script: `scripts/carga_bronze.py`
- Incluye auditoria tecnica: `fecha_carga` y `fuente_archivo`

### Silver (Clean / Staging)

- Tabla esperada: `silver.stg_atenciones_cajamarca`
- Transformacion: `SELECT silver.sp_transform_bronze_to_silver();`

### Gold (Analytics)

- Modelo dimensional (star schema) para consumo BI/analitica
- Dimensiones/hechos dependen de los objetos SQL ya desplegados en tu BD

---

## Estructura del proyecto

```text
PROYECTO_PRODUCTIVO_IIB/
├── README.md
├── requirements.txt
├── .gitignore
├── scripts/
│   ├── extraer_cajamarca.py
│   ├── carga_bronze.py
│   ├── orquestador.py
│   └── pipeline.sh
└── utils/
    ├── __init__.py
    └── logger.py
```

Adicionalmente, en tu entorno local debes crear:

- `.SIS/` (entorno virtual)
- `.secrets/` (credenciales/configuracion local, no versionada)
- `data/raw/` y `data/processed/` (insumos/salidas locales, no versionados)

---

## Requisitos previos

- Python 3.11+ recomendado (3.8+ minimo)
- PostgreSQL accesible (ej. Supabase)
- Usuario con permisos para:
  - `CREATE SCHEMA`
  - `INSERT/UPDATE/DELETE` en esquemas del pipeline
  - ejecutar procedimientos almacenados en `silver`/`meta` (si aplican)

---

## 1) Clonar y preparar entorno

```bash
git clone <URL_DEL_REPOSITORIO>
cd PROYECTO_PRODUCTIVO_IIB

mkdir -p data/raw data/processed .secrets
python -m venv .SIS
source .SIS/bin/activate
pip install -r requirements.txt
```

> En Windows (PowerShell): `.SIS\Scripts\Activate.ps1`

---

## 2) Configuracion de secretos y rutas

Este proyecto espera configuracion local dentro de `.secrets/`.

### 2.1 Archivo `.secrets/.env`

```env
DB_URL=postgresql://USUARIO:CLAVE@HOST:5432/postgres
```

### 2.2 Archivo `.secrets/.toml`

Usado por `scripts/extraer_cajamarca.py` para localizar entrada/salida:

```toml
[paths]
data_raw = "data/raw"
processed_data = "data/processed"
```

### 2.3 Modulo `.secrets/db_connection.py`

El script `scripts/carga_bronze.py` importa `get_engine` desde `db_connection`.
Si no existe en tu entorno, crea este archivo:

```python
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine
import os

ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(ENV_PATH)

def get_engine():
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise ValueError("No se encontro DB_URL en .secrets/.env")
    return create_engine(db_url)
```

---

## 3) Preparar datos de entrada

Coloca tus CSV originales en `data/raw/`.

El script de extraccion busca archivos por anio con el patron:

- `*2017*.csv`
- `*2018*.csv`
- etc.

Ejemplo de nombres validos:

- `atenciones_2017.csv`
- `SIS_2018_data.csv`

---

## 4) Ejecutar pipeline

Tienes 3 formas, segun tu necesidad:

### Opcion A: Ejecucion completa (recomendada)

```bash
python scripts/orquestador.py
```

Esto ejecuta:
1. `scripts/extraer_cajamarca.py`
2. `scripts/carga_bronze.py`

### Opcion B: Ejecucion por pasos

```bash
python scripts/extraer_cajamarca.py
python scripts/carga_bronze.py
```

### Opcion C: Wrapper bash

```bash
bash scripts/pipeline.sh
```

---

## 5) Paso SQL en la base de datos (Silver/Gold)

Luego de cargar Bronze, ejecuta en PostgreSQL:

```sql
SELECT silver.sp_transform_bronze_to_silver();
```

Si tu despliegue incluye transformaciones adicionales a Gold, ejecutalas segun tus scripts SQL de base de datos.

---

## 6) Verificacion rapida

En tu cliente SQL, valida que existan datos:

```sql
SELECT COUNT(*) FROM bronze.atenciones_sis_raw;
SELECT * FROM bronze.atenciones_sis_raw LIMIT 10;
```

Y, si aplica:

```sql
SELECT COUNT(*) FROM silver.stg_atenciones_cajamarca;
```

---

## Solucion de problemas comunes

- `No se encontro DB_URL`: revisa `.secrets/.env`.
- `No se encontro archivo para el anio ...`: confirma nombre y ubicacion de CSV en `data/raw`.
- Error de conexion a BD: valida host/puerto/credenciales y whitelist de IP en Supabase.
- Error por objetos SQL faltantes (`silver.*`, `meta.*`): despliega primero el esquema/procedimientos en la base de datos.

---

## Buenas practicas para replicar

- Mantener `.SIS/`, `.secrets/` y `data/` fuera de Git (ya estan ignorados).
- Ejecutar siempre desde la raiz del proyecto.
- Procesar primero `raw -> processed -> bronze -> silver/gold` para evitar inconsistencias.
