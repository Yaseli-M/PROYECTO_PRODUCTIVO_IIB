import sys
from pathlib import Path
from sqlalchemy import text

# Añadir .secrets al path para importar db_connection
raiz = Path(__file__).resolve().parent.parent / ".secrets"
sys.path.append(str(raiz))
from db_connection import get_engine

engine = get_engine()

#separando procesor de loggin inicio y fin para extraccion de datos y carga bronze

#inicio extraccion
def inicio_extraccion_log(proceso, estado, archivo_origen, usuario, fecha_inicio):
    with engine.connect() as conn:
        query = text(
            """
            INSERT INTO meta.pipeline_log(proceso, estado, archivo_origen, usuario, fecha_inicio)
            VALUES (:proceso, :estado, :archivo_origen, :usuario, :fecha_inicio)
            RETURNING id
            """
        )
        result = conn.execute(
            query, {
                "proceso": proceso,
                "estado": estado,
                "archivo_origen": archivo_origen,
                "usuario": usuario,
                "fecha_inicio": fecha_inicio
            }
        )
        conn.commit()
        return result.scalar()


def fin_extraccion_log(log_id, estado, fecha_fin, registros_procesados, detalles):
    with engine.connect() as conn:
        query = text(
            """
            UPDATE meta.pipeline_log
            SET estado = :estado,
                fecha_fin = :fecha_fin,
                registros_procesados = :registros_procesados,
                detalles = :detalles
            WHERE id = :id
            """
        )
        conn.execute(
            query, {
                "estado": estado,
                "fecha_fin": fecha_fin,
                "registros_procesados": registros_procesados,
                "detalles": detalles,
                "id": log_id
            }
        )
        conn.commit()




def registrar_log(proceso, estado, detalles=""):
    with engine.connect() as conn:
        query = text("""
            INSERT INTO meta.pipeline_log (proceso, estado, detalles, fecha_inicio)
            VALUES (:proceso, :estado, :detalles, NOW())
        """)
        conn.execute(query, {"proceso": proceso, "estado": estado, "detalles": detalles})
        conn.commit()

def iniciar_proceso(proceso, archivo_origen, usuario):
    with engine.connect() as conn:
        query = text("""
            INSERT INTO meta.pipeline_log (proceso, estado, archivo_origen, usuario, fecha_inicio)
            VALUES (:proceso, 'iniciado', :archivo, :usuario, NOW())
            RETURNING id
        """)
        result = conn.execute(query, {"proceso": proceso, "archivo": archivo_origen, "usuario": usuario})
        conn.commit()
        return result.scalar()

def finalizar_proceso(log_id, estado, registros_procesados=0, detalles=None):
    with engine.connect() as conn:
        query = text("""
            UPDATE meta.pipeline_log
            SET estado = :estado,
                fecha_fin = NOW(),
                registros_procesados = :registros,
                detalles = :detalles
            WHERE id = :id
        """)
        conn.execute(query, {
            "estado": estado,
            "registros": registros_procesados,
            "detalles": detalles,
            "id": log_id
        })
        conn.commit()
