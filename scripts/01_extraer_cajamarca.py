import logging
import sys
import tomllib
from pathlib import Path
import pandas as pd

# Configuración de log
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Cargar configuración desde .toml
# Ajuste: el script está en scripts/, el .toml está en .secrets/
CONFIG_PATH = Path("secrets/.toml")
with open(CONFIG_PATH, "rb") as f:
    config = tomllib.load(f)

INPUT_DIR = Path(config["paths"]["data_raw"])
OUTPUT_DIR = Path(config["paths"]["processed_data"])

# Importar logger del proyecto (asumiendo utils.logger)
raiz = Path(__file__).resolve().parent.parent
sys.path.append(str(raiz))
from utils.logger import iniciar_proceso, finalizar_proceso

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
archivos = list(INPUT_DIR.glob("*.csv"))

if not archivos:
    print(f"No se encontraron archivos en {INPUT_DIR}")
else:
    # Solicitud de usuario
    usuario = input("Ingrese el nombre/correo de la persona que realiza la carga: ")
    print(f"Iniciando extracción de registros de CAJAMARCA por {usuario}...")

    for i, archivo in enumerate(archivos):
        año = archivo.name.split('_')[2] if len(archivo.name.split('_')) > 2 else "unknown"
        output_file = OUTPUT_DIR / f"cajamarca_{año}.csv"
        
        log_id = iniciar_proceso('extraer_cajamarca', archivo.name, usuario)
        print(f"\n [%s/%s] Analizando: %s" % (i+1, len(archivos), archivo.name))
        
        try:
            if output_file.exists(): output_file.unlink()
            
            chunks = pd.read_csv(
                archivo, 
                chunksize=150000, 
                sep=',', 
                encoding='utf-8', 
                on_bad_lines='skip', 
                engine='c',
                low_memory=False,
                quoting=3
            )
            
            total_records = 0
            for num_chunk, chunk in enumerate(chunks):
                df_cajamarca = chunk[chunk['REGION'].astype(str).str.contains('CAJAMARCA', case=False, na=False)]
                
                if not df_cajamarca.empty:
                    total_records += len(df_cajamarca)
                    df_cajamarca.to_csv(
                        output_file, 
                        mode='a', 
                        index=False, 
                        header=not output_file.exists(),
                        encoding='utf-8'
                    )
                
                if num_chunk % 5 == 0:
                    print(f"   ... procesando bloque {num_chunk}")

            finalizar_proceso(log_id, 'completado', registros_procesados=total_records)
            print(f"   Generado: {output_file}")

        except Exception as e:
            finalizar_proceso(log_id, 'error', detalle_error=str(e))
            print(f" Error al procesar {archivo.name}: {e}")

    print("\n" + "="*40)
    print(f" ¡PROCESO COMPLETADO!")
    print("="*40)
