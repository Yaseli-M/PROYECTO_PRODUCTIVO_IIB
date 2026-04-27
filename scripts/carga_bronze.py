from pathlib import Path
import pandas as pd
import sys
import logging
from sqlalchemy import text
from datetime import datetime

# Importar logger y db_connection
raiz = Path(__file__).resolve().parent.parent
secrets_dir = raiz / ".secrets"
sys.path.append(str(raiz))
sys.path.append(str(secrets_dir))

from db_connection import get_engine
from utils.logger import iniciar_proceso, finalizar_proceso

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

engine = get_engine()

def get_db_size_mb():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT pg_database_size(current_database()) / (1024 * 1024);")).scalar()
        return result

# Ruta data
processed_dir = raiz / "data" / "processed"
archivos = list(processed_dir.glob("*.csv"))

if not archivos:
    print("No hay archivos procesados en data/processed/")
    sys.exit()

# Menú de selección
print("\nArchivos disponibles:")
for i, f in enumerate(archivos):
    print(f"{i+1}: {f.name}")

seleccion = input("\nSeleccione archivos (ej: 1,2,3 o 'all'): ")
archivos_seleccionados = archivos if seleccion.lower() == 'all' else [archivos[int(i.strip())-1] for i in seleccion.split(',')]

usuario = input("Ingrese su usuario (o presione Enter para 12513514@continental.edu.pe): ") or "12513514@continental.edu.pe"

for ruta_csv in archivos_seleccionados:
    current_size = get_db_size_mb()
    if current_size > 400: # Límite de seguridad 400MB
        logger.error(f"¡PELIGRO! Base de datos casi llena: {current_size}MB. Abortando.")
        sys.exit(1)
        
    print(f"\nProcesando: {ruta_csv.name} (DB Size: {current_size}MB)")
    log_id = iniciar_proceso('carga_bronze', ruta_csv.name, usuario)
    
    try:
        df = pd.read_csv(ruta_csv)
        df.columns = [c.lower().replace(" ", "_").replace(".", "") for c in df.columns]
        df['fecha_carga'] = datetime.now()
        df['fuente_archivo'] = str(ruta_csv.name)
        
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))
            df.to_sql(
                name='atenciones_sis_raw', 
                con=engine, 
                schema='bronze', 
                if_exists='replace', 
                index=False,
                chunksize=500
            )
            conn.commit()
            
        finalizar_proceso(log_id, 'completado', registros_procesados=len(df), detalles=f"Cargados {len(df)} registros desde {ruta_csv.name}")
        print(f"🚀 ¡Carga de {ruta_csv.name} completada!")
        
    except Exception as e:
        finalizar_proceso(log_id, 'error', detalles=f"Error al cargar {ruta_csv.name}: {e}")
        print(f"❌ Error al cargar {ruta_csv.name}: {e}")
