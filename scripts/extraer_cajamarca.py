import logging
import sys
import tomllib
from pathlib import Path

import pandas as pd

raiz = Path(__file__).resolve().parent.parent
sys.path.append(str(raiz))

from utils.logger import registrar_log, inicio_extraccion_log, fin_extraccion_log

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

CONFIG_PATH = raiz / ".secrets" / ".toml"
with open(CONFIG_PATH, "rb") as f:
    config = tomllib.load(f)

INPUT_DIR = Path(config["paths"]["data_raw"])
OUTPUT_DIR = Path(config["paths"]["processed_data"])


def filtrar_por_anio(anio):

    log_id = inicio_extraccion_log(
        proceso=f"FILTRADO_CAJAMARCA_{anio}",
        estado="INICIO",
        archivo_origen=f"atenciones_{anio}.csv",
        usuario="sistema",
        fecha_inicio=pd.Timestamp.now(),
    )

    logger.info(f"Iniciando filtrado para el año {anio}...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    archivos = list(INPUT_DIR.glob(f"*{anio}*.csv"))
    if not archivos:
        raise FileNotFoundError(f"No se encontró archivo para el año {anio} en {INPUT_DIR}")

    archivo = archivos[0]
    output_file = OUTPUT_DIR / f"atenciones_cajamarca_{anio}.csv"

    if output_file.exists():
        output_file.unlink()

    chunks = pd.read_csv(
        archivo,
        chunksize=150000,
        sep=",",
        encoding="utf-8",
        on_bad_lines="skip",
        engine="c",
        low_memory=False,
        quoting=3,
    )

    total_records = 0
    for chunk in chunks:
        df_cajamarca = chunk[chunk["REGION"].astype(str).str.contains("CAJAMARCA", case=False, na=False)]
        if not df_cajamarca.empty:
            total_records += len(df_cajamarca)
            df_cajamarca.to_csv(
                output_file,
                mode="a",
                index=False,
                header=not output_file.exists(),
                encoding="utf-8",
            )

    """ registrar_log(
        "FILTRADO_CAJAMARCA",
        "EXITO",
        f"Año {anio}: {total_records} registros guardados en {output_file}",
    )
    logger.info(f"Procesado año {anio} con éxito. Total: {total_records}") """

    fin_extraccion_log(
        log_id=log_id,
        estado="EXITO",
        fecha_fin=pd.Timestamp.now(),
        registros_procesados=total_records,
        detalles=f"Año {anio}: {total_records} registros guardados en {output_file}"
    )


if __name__ == "__main__":
    print("Selecciona los años a procesar (ejemplo: 2017,2018):")
    seleccion = input("> ")
    anios = [a.strip() for a in seleccion.split(",") if a.strip()]

    for anio in anios:
        try:
            filtrar_por_anio(anio)
        except Exception as e:
            registrar_log("FILTRADO_CAJAMARCA", "ERROR", str(e))
            logger.error(f"Fallo en año {anio}: {e}")
