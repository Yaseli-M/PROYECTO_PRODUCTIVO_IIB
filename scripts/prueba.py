import os
from pathlib import Path



ruta_actual = Path(__file__).resolve()
print(ruta_actual.name)
print(f"Ruta del script actual: {ruta_actual}")