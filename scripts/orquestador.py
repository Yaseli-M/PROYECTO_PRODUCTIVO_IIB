import sys
import subprocess
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent


def run_script(script_name):
    script_path = SCRIPT_DIR / script_name
    result = subprocess.run([sys.executable, str(script_path)])
    if result.returncode != 0:
        raise RuntimeError(f"Fallo la ejecución de {script_name}")

if __name__ == "__main__":
    try:
        print("Iniciando extracción de CAJAMARCA...")
        run_script("extraer_cajamarca.py")
        print("Extracción finalizada. Iniciando carga Bronze...")
        run_script("carga_bronze.py")
        print("Pipeline completado.")
    except Exception as e:
        print(f"Error en orquestador: {e}")
        sys.exit(1)