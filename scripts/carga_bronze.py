from pathlib import Path
import pandas as pd
import sys
import dotenv
from sqlalchemy import create_engine, text
import os
import urllib.parse
from datetime import datetime
# from pyspark.sql import SparkSession

#    datos para la conexion mediante psycopg2, pero se usara sqlalchemy para la carga de datos a postgres

#importando coneccion db
ruta_env = Path(__file__).resolve().parent.parent / ".secrets" / ".env"
dotenv.load_dotenv(dotenv_path=ruta_env)

user = os.getenv("DB_USER")
password = urllib.parse.quote_plus(os.getenv("DB_PASSWORD"))
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
db = os.getenv("DB_NAME")
# url_secrets = (url_base / "../.secrets").resolve()
# agregando el path a sys.path para importar el módulo de conexión
""" sys.path.append(str(url_secrets))
from db_connection import get_db_connection """
db_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
engine = create_engine(db_url)


# password = os.getenv("DB_PASSWORD")
# password_scape = urllib.parse.quote_plus(password)

# db_url = f"postgresql://postgres.qhrueithzijzmysinidk:{password_scape}@aws-0-us-west-2.pooler.supabase.com:5432/postgres"

# ruta data
ruta_csv = Path(__file__).resolve().parent.parent / "data" / "processed" / "cajamarca_raw.csv"

# Cargar los datos desde el CSV
df = pd.read_csv(ruta_csv)
df.columns = [c.lower().replace(" ", "_").replace(".", "") for c in df.columns]
df['fecha_carga'] = datetime.now()
df['fuente_archivo'] = ''

try:
    with engine.connect() as conn:
        # Esto confirma que el "túnel" a la base de datos está abierto
        print("✅ Conexión a la base de datos exitosa (vía SQLAlchemy Engine).")
        # print(f"⏳ Iniciando carga de {len(df)} filas...")

        # --- ESPACIO PARA LA CARGA DE DATOS BRONZE ---
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))
        conn.commit()

        # 3. Cargar a la base de datos
        df.to_sql(
            name='atenciones_sis_raw', 
            con=engine, 
            schema='bronze', 
            if_exists='replace', # 'replace' sobrescribe la tabla, 'append' agrega datos
            index=False,         # No queremos que el índice de Pandas sea una columna
            chunksize=500        # Envía los datos en bloques de 500 para no saturar Supabase
        )
        print("🚀 ¡Carga a 'bronze.atenciones_sis_raw' completada con éxito!")

        
        # --- FIN ESPACIO PARA LA CARGA DE DATOS BRONZE ---

except Exception as e:
    print(f"❌ No se pudo conectar a la base de datos.")
    print(f"Error técnico: {e}")