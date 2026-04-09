import sys
import pandas as pd
from dotenv import load_dotenv
from psycopg2 import Error
import os
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
import datetime

from etl_logger import registrar_log
# --- 1. CONFIGURACIÓN ---

#origen_schema = 'voriginal'
#destino_schema = 'vmodelada'

def etl_dim_victimas(engine, origen_schema, destino_schema):
    tabla_origen = "data"
    tabla_destino = "dim_victima"
    total_nuevas = 0  # Inicializar contador

    print(f"\n---Iniciando ETL para '{tabla_destino}' ---")

    try:
        with engine.connect() as connection:
            # --- EXTRACT ---
            columnas_victimas = [
                "nombres_victima", "apellidos_victima", "edad_victima", "dpi_victima", "sexo_descripcion", "estado_civil_victima",
                '"direc_cod_depto_victima-2"', "cod_muni_hecho"
            ]
            sql_victimas = f'SELECT {", ".join(columnas_victimas)} FROM "{origen_schema}"."{tabla_origen}"'
            df_victimas = pd.read_sql(sql_victimas, connection) 

            df_lookup_direccion = pd.read_sql_table('dim_direccion', connection, schema=destino_schema)
            print(f" 	 -> Datos extraídos: {len(df_victimas)} víctimas origen, {len(df_lookup_direccion)} direcciones lookup.")

            # --- TRANSFORM ---
            print(" 	 -> Transformando datos...")
            df_victimas.rename(columns={
                'direc_cod_depto_victima-2': 'cnegociod',
                'cod_muni_hecho': 'cnegociom',
                'dpi_victima': 'victima_f_original',
                'nombres_victima': 'nombres',
                'apellidos_victima': 'apellidos',
                'edad_victima': 'edad',
                'sexo_descripcion': 'sexo',
                'estado_civil_victima': 'estado_civil'
            }, inplace=True)

            # Limpieza de clave de negocio (victima_f_original)
            df_victimas['victima_f_original_int'] = pd.to_numeric(df_victimas['victima_f_original'], errors='coerce').astype('Int64')
            df_victimas['victima_f_original'] = df_victimas['victima_f_original_int'].astype(str).str.strip().replace('<NA>', '')

            df_victimas.dropna(subset=['victima_f_original'], inplace=True)
            df_victimas = df_victimas[df_victimas['victima_f_original'] != '']
            df_victimas.drop_duplicates(subset=['victima_f_original'], inplace=True, keep='first')

            # Limpieza de atributos
            df_victimas['nombres'] = df_victimas['nombres'].str.strip().str.title()
            df_victimas['apellidos'] = df_victimas['apellidos'].str.strip().str.title()
            df_victimas['sexo'] = df_victimas['sexo'].str.strip().str.title()
            df_victimas['edad'] = pd.to_numeric(df_victimas['edad'], errors='coerce').fillna(0).astype('int64')

            # FASE DE COMPARACIÓN
            print(" 	 -> 2. Comparando con víctimas existentes en el Data Warehouse...")
            sql_existentes = f'SELECT victima_f_original FROM "{destino_schema}"."{tabla_destino}"'
            df_existentes = pd.read_sql(sql_existentes, connection)
            set_existentes = set(df_existentes['victima_f_original'].astype(str)) 

            
            df_nuevas_victimas = df_victimas[
                ~df_victimas['victima_f_original'].astype(str).isin(set_existentes)
            ].copy()

            total_nuevas = len(df_nuevas_victimas)
            if total_nuevas == 0:
                print(" 	 -> No se encontraron víctimas nuevas para agregar.")
                print(f"✅ ETL Incremental para '{tabla_destino}' completado.")
                return 0 # Devolver 0

            print(f" 	 -> Se encontraron {total_nuevas} víctimas nuevas para cargar.")

            # LOOKUP DE DIRECCIÓN (sólo para las nuevas) 
            print(" 	 -> Realizando lookup a dirección...")
            df_lookup_direccion.drop_duplicates(subset=['cnegociod', 'cnegociom'], inplace=True, keep='first')
            
            # Asegurar tipos para el merge
            for col in ['cnegociod', 'cnegociom']:
                # Aplicar a df_nuevas_victimas ---
                df_nuevas_victimas[col] = pd.to_numeric(df_nuevas_victimas[col], errors='coerce').fillna(0).astype('int64')
                df_lookup_direccion[col] = pd.to_numeric(df_lookup_direccion[col], errors='coerce').fillna(0).astype('int64')

            # Usar df_nuevas_victimas para el merge 
            df_transformado = pd.merge(
                df_nuevas_victimas,
                df_lookup_direccion[['id_direccion', 'cnegociod', 'cnegociom']],
                on=['cnegociod', 'cnegociom'],
                how='left' 
            )

            columnas_finales = ['nombres', 'apellidos', 'edad', 'sexo', 'estado_civil', 'id_direccion', 'victima_f_original']
            df_final = df_transformado[columnas_finales]
            print(" 	 -> Transformación completada.")

            # --- LOAD ---
            print(" 	 -> Cargando datos...")
            
            df_final.to_sql(
                tabla_destino,
                engine,
                schema=destino_schema,
                if_exists='append', 
                index=False
            )
            
            print(f" 	 -> Carga finalizada. Se cargaron {total_nuevas} registros.")
            print(f"✅ ETL para '{tabla_destino}' completado.")
            return total_nuevas #Devolver el total

    except Exception as e:
        print(f"🔥 ERROR en el ETL de '{tabla_destino}': {e}")
        raise


def main():
    load_dotenv()
    
    db_user = os.getenv("model_user_db")
    db_password = os.getenv("model_password_db")
    db_host = os.getenv("db_host")
    db_port = os.getenv("db_port")
    db_name = os.getenv("db_name")
    origen_schema = os.getenv("origen_schema")
    destino_schema = os.getenv("destino_schema")

    if not all([db_user, db_password, db_host, db_port, db_name, origen_schema, destino_schema]):
        print("ERROR: Faltan variables de entorno en el archivo .env.")
        sys.exit(1)

    engine = None
    try:
        connection_string = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(connection_string)

        
        with engine.connect() as connection:
            print("✅ Conexión a la base de datos establecida.")

        proceso_etl = {"dim_victima": etl_dim_victimas}
        
        for nombre, funcion_etl in proceso_etl.items():
            fecha = datetime.datetime.now(datetime.timezone.utc)
            try:
                #recibirá 0 o el total de nuevas víctimas
                registros = funcion_etl(engine, origen_schema, destino_schema)
                print(f"✔️ Proceso '{nombre}' finalizado con éxito. {registros} registros afectados.")
                registrar_log(engine, nombre, fecha, 'Éxito', registros)
            except Exception as e:
                print(f"❌ Error en '{nombre}': {e}")
                registrar_log(engine, nombre, fecha, 'Fallo', error=str(e))
                print(f"Error fatal en '{nombre}'. El proceso principal se detendrá.")
                sys.exit(1)

        print("\n🎉 El proceso ETL ha finalizado exitosamente.")
    except Exception as e:
        print(f"\n🔥 El proceso ETL se detuvo debido a un error crítico: {e}")
        sys.exit(1)
    finally:
        if engine:
            engine.dispose()
            print("\nConexión a la base de datos cerrada.")

if __name__ == "__main__":
    main()