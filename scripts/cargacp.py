from multiprocessing import connection
import os
import pandas as pd
from sqlalchemy import create_engine, text
import logging
import datetime
from dotenv import load_dotenv 

# Cargar las variables del .env al entorno ---
load_dotenv() 

# Variables de entorno para la conexión a la base de datos
db_user = os.getenv('load_user_db')
db_password = os.getenv('load_password_db')
db_host = os.getenv('db_host')
db_port = os.getenv('db_port')
db_name = os.getenv('db_name')

# Configuraciones
staging_schema = 'voriginal'  # Esquema destino
log_schema = 'public'     # Esquema de la bitácora
csv_folder = '../data/' # Carpeta con archivos CSV

# Configurar logging (para la consola/archivo)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

#  2. FUNCIONES AUXILIARES

def check_env_vars() -> bool:
    """Verifica que todas las variables de entorno requeridas estén definidas."""
    vars_ok = all([db_user, db_password, db_host, db_port, db_name]) # all devuelve True si todos son True
    if not vars_ok: # Si falta alguna variable
        logging.error("❌ Faltan variables de entorno (.env). Verifica configuración.")
    return vars_ok


def get_engine():
    """Crea el engine de conexión a la base de datos."""
    conn_str = f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    return create_engine(conn_str) # Crear el engine de SQLAlchemy


def clear_and_load_table(connection, df, schema: str, table: str) -> int: 
    """
    Elimina los registros existentes e inserta nuevos datos desde un DataFrame.
    Devuelve la cantidad de filas cargadas.
    """
    # Eliminar registros existentes
    connection.execute(text(f"DELETE FROM {schema}.{table}"))
    # Insertar nuevos datos
    df.to_sql(table, con=connection, schema=schema, if_exists='append', index=False)
    # Confirmar cantidad cargada
    result = connection.execute(text(f"SELECT COUNT(*) FROM {schema}.{table}"))
    return result.scalar() or 0


def log_etl_execution(engine, table, file, start, end, rows_csv, rows_db, status, error_msg=None):
    """Registra la ejecución del proceso en la bitácora (log_carga_staging)."""
   
    try:
        log_entry = {
            "timestamp_inicio": start,
            "timestamp_fin": end,
            "nombre_tabla": table,
            "nombre_archivo": file,
            "filas_leidas_csv": rows_csv,
            "filas_cargadas_db": rows_db,
            "estado": status,
            "mensaje_error": error_msg
        }

        query = text(f"""
            INSERT INTO {log_schema}.log_carga_staging (
                timestamp_inicio, timestamp_fin, nombre_tabla, nombre_archivo,
                filas_leidas_csv, filas_cargadas_db, estado, mensaje_error
            ) VALUES (
                :timestamp_inicio, :timestamp_fin, :nombre_tabla, :nombre_archivo,
                :filas_leidas_csv, :filas_cargadas_db, :estado, :mensaje_error
            )
        """)

    
        with engine.connect() as conn:
            conn.execute(query, log_entry) # Insertar registro
            conn.commit()
    except Exception as e:
        logging.error(f"⚠️  No se pudo escribir en bitácora: {e}")



#  3. FUNCIÓN PRINCIPAL ETL

def process_csv_file(engine, file_name: str):
    """Procesa un solo archivo CSV: carga a BD y registra log."""
    table = file_name.replace('.csv', '') 
    file_path = os.path.join(csv_folder, file_name) # Ruta completa al archivo

    start_time = datetime.datetime.now(datetime.timezone.utc) # Marca de tiempo de inicio
    status = "Éxito"
    error_msg = None
    rows_read = rows_loaded = 0 # Inicializar contadores

    logging.info(f"🔄 Cargando archivo: {file_name} → {staging_schema}.{table}")

    try:
        df = pd.read_csv(file_path) # Leer CSV
        rows_read = len(df) # Contar filas leídas

        if df.empty: # Verificar si el DataFrame está vacío
            logging.warning(f"⚠️ {file_name} está vacío. Se omite la carga.")
            status = "Vacío"
            return  # Saltamos bitácora, opcionalmente podrías registrarlo igual

        with engine.begin() as conn: # Iniciar transacción
            rows_loaded = clear_and_load_table(conn, df, staging_schema, table) # Cargar datos

        logging.info(f"✅ {table} cargado correctamente ({rows_loaded} filas).") # Éxito

    except Exception as e:
        status = "Error"
        error_msg = str(e)
        logging.error(f"❌ Error al procesar {file_name}: {e}")

    finally:
        end_time = datetime.datetime.now(datetime.timezone.utc)
        log_etl_execution(engine, table, file_name, start_time, end_time, rows_read, rows_loaded, status, error_msg)


def main():
    """Controlador principal: carga todos los CSVs al esquema staging."""
    if not check_env_vars(): # Verificar variables de entorno
        return
    if not os.path.exists(csv_folder): # Verificar carpeta de datos
        logging.error(f"📂 Carpeta no encontrada: {csv_folder}")
        return

    engine = get_engine()
    logging.info("🚀 Conexión a la base de datos establecida.")

    try:
        csv_files = [f for f in os.listdir(csv_folder) if f.endswith('.csv')] # Listar archivos CSV
        if not csv_files: # Si no hay archivos
            logging.warning("⚠️ No se encontraron archivos CSV en la carpeta.")
            return

        for file in csv_files: # Procesar cada archivo
            process_csv_file(engine, file) # Procesar archivo individual

    except Exception as e:
        logging.error(f"Error general del proceso: {e}")

    finally:
        engine.dispose()
        logging.info("🔚 Conexión a la base de datos cerrada.")



#  4. EJECUCIÓN

if __name__ == "__main__":
    main()