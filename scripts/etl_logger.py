from sqlalchemy import text
import datetime

def registrar_log(engine, nombre_proceso, fecha_inicio, estado, registros=0, error=''):
    """
    Inserta un registro en la tabla de auditoría log_etl.
    """
    fecha_fin = datetime.datetime.now(datetime.timezone.utc)
    
    sql = text("""
        INSERT INTO vmodelada.etl_bitacora 
        (nombre_proceso, fecha_ejecucion, estado, registros_cargados, mensaje_error)
        VALUES (:proceso, :fecha, :estado, :registros, :error)
    """)
    
    try:
        with engine.connect() as connection:
            connection.execute(sql, {
                'proceso': nombre_proceso,
                'fecha': fecha_inicio,
               
                'estado': estado,
                'registros': registros,
                'error': str(error)[:5000] # Limita el tamaño del mensaje de error
            })
            connection.commit()
        print(f"   -> Log para '{nombre_proceso}' registrado como '{estado}'.")
    except Exception as e:
        print(f"🔥 ERROR al intentar registrar el log para '{nombre_proceso}': {e}")

