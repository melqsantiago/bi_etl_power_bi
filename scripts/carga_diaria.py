import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from etl_logger import registrar_log
# Importar las funciones específicas de los otros archivos
from dim_vic import etl_dim_victimas
from hechos import etl_hechos_casos

import datetime
import sys

def main():
    """
    Función principal que orquesta la carga periódica 
    """
    # Cargar Variables de Entorno 
    
    load_dotenv()
    print("=INICIANDO CARGA PERIÓDICA DE DATOS=")
    
        
    db_user = os.getenv("model_user_db")
    db_password = os.getenv("model_password_db")
    db_host = os.getenv("db_host")
    db_port = os.getenv("db_port")
    db_name = os.getenv("db_name")
    origen_schema = os.getenv("origen_schema")
    destino_schema = os.getenv("destino_schema")
    

    # Verificación para asegurar que todas las variables fueron encontradas
    if not all([db_user, db_password, db_host, db_port, db_name]):
        print("ERROR: Faltan una o más variables de entorno en el archivo .env. Asegúrate de que estén definidas.")
        return # Detiene la ejecución si faltan credenciales

    try:
        # Configurar Conexión ---
        engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

        # Ejecutar ETLs en Orden
        # Primero, se actualiza la dimensión dinámica de víctimas.
        #etl_dim_victimas(engine)
        
        # Segundo, con todas las dimensiones listas, se carga la tabla de hechos.
        #etl_hechos_casos(engine)

        procesos_etl = {
            "dim_victima": lambda: etl_dim_victimas(engine, origen_schema, destino_schema),
            "hechos_casos": lambda: etl_hechos_casos(engine, origen_schema, destino_schema)
            
        }

        for nombre, funcion_etl in procesos_etl.items():
            fecha = datetime.datetime.now(datetime.timezone.utc)
            try:
                # Ejecuta la función y captura el número de registros
                registros = funcion_etl()
                registrar_log(engine, nombre, fecha, 'Exitoso', registros)
            except Exception as e:
                # Si falla, registra el error
                print(f" ERROR en '{nombre}': {e}")

                registrar_log(engine, nombre, fecha, 'Fallido', error=e)
                print(f" ERROR FATAL en '{nombre}'. El proceso principal se detendrá.")
                sys.exit(1) # Detiene la ejecución si 

        print("= CARGA PERIÓDICA COMPLETADA EXITOSAMENTE =")

    except Exception as e:
        print(f" EL PROCESO ETL SE DETUBO DEBIDO A UN ERROR")
        sys.exit(1) # Detiene la ejecución si falla alguna ETL
    finally:
        if engine:
            engine.dispose()
            print(" Conexion a la base de datos cerrada")

if __name__ == "__main__":
    main()
