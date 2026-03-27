# Pipeline de Datos - Arquitectura Medallion (Capa Bronze)

Este proyecto implementa un flujo de ingeniería de datos siguiendo la metodología Medallion. El objetivo principal es la extracción de datos de salud (SIS) y su carga eficiente en una base de datos PostgreSQL (Supabase), asegurando la trazabilidad, auditoría y calidad desde el origen.

## Estructura del Proyecto

Para que el pipeline funcione correctamente y las rutas relativas no se rompan, el proyecto debe mantener la siguiente jerarquía de archivos:

```
PROYECTO_PRODUCTIVO_IIB/
├── .secrets/              # Credenciales y configuración sensible (no se sube al repo)
│   └── .env               # Variables de entorno (No se sube al repo)
├── .SIS/                  # Entorno virtual de Python (no se sube al repo)
├── data/                  # Almacenamiento local de archivos (no se sube el repo)
│   ├── raw/               # Archivos CSV originales (Fuente de entrada)
│   └── processed/         # Archivos procesados o temporales
├── scripts/               # Scripts de ejecución del Pipeline
│   ├── 01_extraer_cajamarca.py
│   └── carga_bronze.py
├── .gitignore             # Archivos y carpetas excluidos de Git
├── requirements.txt       # Librerías necesarias para el proyecto
└── README.md              # Documentación e instrucciones
```

## Configuración e Instalación

Sigue estos pasos para replicar el entorno de desarrollo local después de clonar el repositorio:

### 1. Preparar las carpetas de datos

Git no rastrea directorios vacíos. Ejecuta el siguiente comando en tu terminal para crear la estructura necesaria:

```bash
mkdir -p data/raw data/processed .secrets
```

### 2. Crear y Activar el Entorno Virtual

Es fundamental usar el entorno virtual incluido en la estructura para evitar conflictos de versiones.

#### En Windows:

```bash
python -m venv .SIS
.SIS\Scripts\activate
```

#### En Linux / macOS:

```bash
python3 -m venv .SIS
source .SIS/bin/activate
```

### 3. Instalar Dependencias

Una vez activado el entorno, instala todas las librerías requeridas mediante el archivo de requerimientos:

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 4. Configurar Variables de Entorno

Crea un archivo llamado **.env** dentro de la carpeta **.secrets/** y añade tu cadena de conexión de Supabase (reemplaza con tus credenciales reales):

```env
DB_URL=postgresql://postgres.[TU_ID_PROYECTO]:[TU_PASSWORD]@aws-0-us-west-2.pooler.supabase.com:5432/postgres
```

# Ejecución del Pipeline

## Paso 1: Carga de archivos fuente

Coloca el archivo CSV de interés (ej. **atenciones_sis.csv**) dentro de la carpeta **data/raw/.**

## Paso 2: Ejecutar carga a Capa Bronze

Desde la raíz del proyecto, ejecuta el script de ingesta:

```bash
python scripts/carga_bronze.py
```

# ¿Qué hace el proceso automáticamente?

- **Gestión de Rutas:** Utiliza pathlib para localizar archivos en data/raw/ de forma dinámica, garantizando compatibilidad entre Windows, Linux y Mac.

- **Auditoría de Datos:** El script inyecta automáticamente dos columnas de metadatos:
  - **fecha_carga:** Timestamp del momento exacto de la inserción.

  - **fuente_archivo:** Nombre del archivo CSV original para mantener el linaje del dato.

- **Carga Masiva:** Crea el esquema bronze (si no existe) y realiza la carga mediante el método to_sql de Pandas optimizado con chunksize.

# Notas de Seguridad y Mantenimiento

- **Exclusiones:** Las carpetas **data/, .secrets/ y .SIS/** están configuradas en el **.gitignore.** Nunca fuerces la subida de estos archivos para proteger la privacidad de los datos y las credenciales.

- **Trazabilidad:** Si necesitas actualizar los datos, el script está configurado para reemplazar la tabla en Bronze, asegurando que siempre trabajes con la versión más reciente del archivo crudo.

# Requisitos del Sistema

- Python 3.8 o superior

- PostgreSQL (Supabase)

- Acceso a internet para instalar dependencias
