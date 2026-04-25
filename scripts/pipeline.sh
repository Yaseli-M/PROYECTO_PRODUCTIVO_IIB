#!/bin/bash

# Pipeline Runner
echo "Iniciando pipeline..."
python3 scripts/orquestador.py
if [ $? -eq 0 ]; then
    echo "Filtro completado. Iniciando carga a Bronze..."
    python3 scripts/carga_bronze.py
else
    echo "El filtrado falló."
    exit 1
fi
