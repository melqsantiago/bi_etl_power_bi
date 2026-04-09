import os
import sys
import pandas as pd
from urllib.parse import quote_plus
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import datetime
from etl_logger import registrar_log

def etl_dim_direccion(engine, origen_schema, destino_schema):
    tabla_deptos = 'oav_renap_departamento'
    tabla_mun = 'oav_renap_municipio'
    dest_table = "dim_direccion"
    total_nuevas = 0  # Inicializar contador

    try:
        with engine.connect() as connection:
            print(f"\n--- Iniciando ETL para '{dest_table}' ---")
            sql_a = f'SELECT id_depto, descripcion as nom_depto FROM "{origen_schema}"."{tabla_deptos}"'
            sql_b = f'SELECT id_depto, id_depto_muni, descripcion as nom_muni FROM "{origen_schema}"."{tabla_mun}"' 
            df_a = pd.read_sql(sql_a, connection) 
            df_b = pd.read_sql(sql_b, connection)
            print(f"✅ Datos extraídos: {len(df_a)} de '{tabla_deptos}' y {len(df_b)} de '{tabla_mun}'.")

            print("--- Iniciando Transformación (T) ---")
            df_transformado = pd.merge(df_a, df_b, on="id_depto", how="outer") # Realiza un outer join para incluir todos los registros

            df_transformado.rename(columns={
                'id_depto': 'cnegociod',
                'id_depto_muni': 'cnegociom',
            }, inplace=True) # 

            print("Comparando con direcciones existentes en el DW...")
            sql_existentes = f'SELECT cnegociod, cnegociom FROM "{destino_schema}"."{dest_table}"'
            df_existentes = pd.read_sql(sql_existentes, connection)

            # Convertir a tuplas para comparación eficiente con sets
            df_transformado['clave_compusta'] = list(zip(df_transformado['cnegociod'], df_transformado['cnegociom']))
            set_existentes = set(zip(df_existentes['cnegociod'], df_existentes['cnegociom']))

            df_nueva_dir = df_transformado[~df_transformado['clave_compusta'].isin(set_existentes)].copy() 

            total_nuevas = len(df_nueva_dir) 
            if total_nuevas == 0:
                print("   -> No se encontraron nuevas direcciones para agregar.")
                print(f"   -> Proceso de ETL para '{dest_table}' finalizado.")
                return 0  # devuelve 0 si no hay nuevas direcciones

            print(f"   -> Se encontraron {total_nuevas} nuevas direcciones para cargar.")

            columnas_finales = ['cnegociod', 'nom_depto', 'cnegociom', 'nom_muni']
            df_final_carga = df_nueva_dir[columnas_finales]
            print("✅ Transformación completada.")

            print("--- Iniciando Carga (L) ---")
            df_final_carga.to_sql(
                dest_table,
                engine,
                schema=destino_schema,
                if_exists='append', # Agregar nuevas filas sin eliminar las existentes
                index=False # No incluir el índice del DataFrame como columna en la tabla
            )
            print(f"✅ Carga finalizada. Se insertaron {total_nuevas} registros.")
            return total_nuevas  # devuelve el total de nuevas direcciones insertadas

    except Exception as e:
        print(f"🔥 ERROR: Ocurrió un error inesperado durante el ETL de '{dest_table}': {e}")
        raise

def etl_dim_tiempo(engine, destino_schema):
       
    tabla_destino = "dim_tiempo"
    total_nuevas = 0

    print(f"\n--- Iniciando ETL para '{tabla_destino}' ---")
    try:
        with engine.connect() as connection:
            df = pd.DataFrame({"fecha": pd.date_range("2023-01-01", "2025-06-01")}) #dataframe se usa para crear rango de fechas

            print("   -> Comparando con fechas existentes en el DW...")
            df_existentes = pd.read_sql(f'SELECT fecha FROM "{destino_schema}"."{tabla_destino}"', connection)
            df_existentes['fecha'] = pd.to_datetime(df_existentes['fecha']) # Asegura que las fechas estén en formato datetime

            df_nueva_fecha = df[~df['fecha'].isin(df_existentes['fecha'])].copy() #isin devuelve booleano y ~ invierte, copy crea copia del dataframe

            total_nuevas = len(df_nueva_fecha)
            if total_nuevas == 0:
                print("   -> No se encontraron nuevas fechas para agregar.")
                print(f"   -> Proceso de ETL para '{tabla_destino}' finalizado.")
                return 0  

            print(f"   -> Se encontraron {total_nuevas} nuevas fechas para cargar.")

            df_nueva_fecha["dia"] = df_nueva_fecha.fecha.dt.day 
            df_nueva_fecha["mes"] = df_nueva_fecha.fecha.dt.month 
            df_nueva_fecha["año"] = df_nueva_fecha.fecha.dt.year
            df_nueva_fecha["trimestre"] = df_nueva_fecha.fecha.dt.quarter
            df_nueva_fecha["nombre_mes"] = df_nueva_fecha.fecha.dt.strftime('%B')
            df_nueva_fecha["nombre_dia"] = df_nueva_fecha.fecha.dt.day_name()
            print("✅ Transformación completada.")

            print("--- Iniciando Carga (L) ---")
            df_nueva_fecha.to_sql(tabla_destino, engine, schema=destino_schema, if_exists='append', index=False)
            
            print(f"✅ ETL para '{tabla_destino}' completado exitosamente. {total_nuevas} registros insertados.")
            return total_nuevas  
    except Exception as e:
        print(f"🔥 ERROR en el ETL de '{tabla_destino}': {e}")
        raise

def etl_dim_violencia(engine, origen_schema, destino_schema):
  
    tabla_destino = "dim_tipo_violencia"
    total_nuevas = 0
    print(f"\n--- Iniciando ETL para '{tabla_destino}' ---")
    try:
        with engine.connect() as connection:
            # --- EXTRACT ---
            sql_tipov = f'SELECT tipo_agresion_descripcion AS nombre_violencia FROM "{origen_schema}"."data"'
            df = pd.read_sql(sql_tipov, connection)
            print(f" -> Datos extraídos: {len(df)} registros para procesar.")

            # --- TRANSFORM ---
            print("-> Transformando y limpiando datos...")
            df['nombre_violencia'] = df['nombre_violencia'].str.strip().str.title() # Normaliza mayúsculas y minúsculas
            df.dropna(subset=['nombre_violencia'], how='all', inplace=True) 
            df.drop_duplicates(subset=['nombre_violencia'], inplace=True, keep='first') 

            df_desconocido = pd.DataFrame([['No especificado']], columns=['nombre_violencia']) 
            df_final = pd.concat([df_desconocido, df], ignore_index=True) 

            print(" -> Comparando con tipo violencia existentes en el Dw... ")
            sql_existentes = f'SELECT nombre_violencia FROM "{destino_schema}"."{tabla_destino}"'
            df_existentes = pd.read_sql(sql_existentes, connection)

            set_existentes = set(df_existentes['nombre_violencia'].astype(str)) 
            df_nuevas = df_final[~df_final['nombre_violencia'].astype(str).isin(set_existentes)].copy() 

            total_nuevas = len(df_nuevas)
            if total_nuevas == 0:
                print(" -> No se encontraron nuevos tipos de violencia para agregar.")
                print(f"-> Proceso de ETL para '{tabla_destino}' finalizado.")
                return 0  
            
            print(f"-> Se encontraron {total_nuevas} nuevos tipos de violencia para cargar.")

            # Aplicar transformación sólo a df_nuevas
            df_nuevas['violencia_f_original'] = (
                df_nuevas['nombre_violencia'].str.upper().str.strip().str.replace(" ", "_") 
            )
            print(f"-> Transformación completada. {total_nuevas} violencias únicas nuevas.")

            # --- LOAD ---
            print(" -> Cargando datos...")
            
            # Cargar sólo df_nuevas
            df_nuevas.to_sql(tabla_destino, engine, schema=destino_schema, if_exists='append', index=False)
            
            print(f"✅ ETL para '{tabla_destino}' completado. Se insertaron {total_nuevas} registros.")
            return total_nuevas  
            
    except Exception as e:
        print(f"🔥 ERROR en el ETL de '{tabla_destino}': {e}")
        raise

def main():
    load_dotenv()

    # CONFIGURACIÓN DESDE VARIABLES DE ENTORNO
    db_user = os.getenv("model_user_db")
    db_password_ori = os.getenv("model_password_db")
    db_host = os.getenv("db_host")
    db_port = os.getenv("db_port")
    db_name = os.getenv("db_name")
    origen_schema = os.getenv("origen_schema")
    destino_schema = os.getenv("destino_schema")

    required_vars = [db_user, db_password_ori, db_host, db_port, db_name, origen_schema, destino_schema]
    if not all(required_vars):
        print(" ERROR: Faltan una o más variables de entorno en el archivo .env. Revisa la configuración.")
        sys.exit(1) # Salir con código de error
        
    db_password = quote_plus(db_password_ori)
    
    engine = None
    try:
        connection_string = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}" 
        engine = create_engine(connection_string)

        with engine.connect() as connection:
            print("✅ Conexión a la base de datos establecida.")
            #connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {destino_schema}"))
            #connection.commit()

        procesos_etl = {
            "dim_direccion": lambda: etl_dim_direccion(engine, origen_schema, destino_schema), 
            "dim_tiempo": lambda: etl_dim_tiempo(engine, destino_schema),
            "dim_violencia": lambda: etl_dim_violencia(engine, origen_schema, destino_schema)
        }

        for nombre, funcion_etl in procesos_etl.items(): 
            fecha = datetime.datetime.now(datetime.timezone.utc)
            try:
                #'registros' tendrá un valor numérico
                registros = funcion_etl()
                
                registrar_log(engine, nombre, fecha, 'Exitoso', registros)
                print(f"✔️  Proceso '{nombre}' finalizado con éxito. {registros} registros afectados.")
            except Exception as e:
                print(f"❌ ERROR en '{nombre}': {e}")
                # 'error' tendrá un mensaje de error
                registrar_log(engine, nombre, fecha, 'Fallido', error=str(e))
                print(f"ERROR FATAL en '{nombre}'. El proceso principal se detendrá.")
                sys.exit(1)
        
        print("\n🎉 El proceso ETL ha finalizado exitosamente.")

    except Exception as e:
        print(f"🔥 ERROR FATAL: El proceso ETL principal se detuvo. Causa: {e}")
        sys.exit(1)
    finally:
        if engine:
            engine.dispose()
            print("Conexión a la base de datos cerrada.")

if __name__ == "__main__":
    main()