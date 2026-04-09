import sys
import pandas as pd
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from etl_logger import registrar_log
import datetime


def diagnosticar_fallos_de_union(df_hechos_post_merge, df_dim, merge_key_hechos, merge_key_dim, id_col_dim, dim_name):
    """
    Función de ayuda para imprimir un diagnóstico claro cuando un merge falla.
    """
    # Filtra las filas donde el ID de la dimensión es Nulo después del merge
    fallos = df_hechos_post_merge[df_hechos_post_merge[id_col_dim].isna()]
    
    if not fallos.empty:
        print(f"\n---  DIAGNÓSTICO DE FALLO PARA '{dim_name}' ---")
        print(f"El merge no encontró ninguna coincidencia. Analicemos por qué.")
        
        # Muestra 5 ejemplos de las claves del DataFrame de hechos que no encontraron pareja
        print(f"\n  1. Ejemplos de claves de 'hechos' que FALLARON:")
        print(fallos[[merge_key_hechos]].head().to_string())
        
        # Muestra 5 ejemplos de las claves que SÍ existen en la tabla de dimensión
        print(f"\n  2. Ejemplos de claves DISPONIBLES en '{dim_name}':")
        print(df_dim[[merge_key_dim]].head().to_string())
        print(f"--- FIN DEL DIAGNÓSTICO PARA '{dim_name}' ---\n")
        print("ACCIÓN: Compara las claves de arriba. Deben ser IDÉNTICAS. Ajusta la lógica de creación de la dimensión o de este script para que coincidan.")


# ETL PARA LA TABLA DE HECHOS
def etl_hechos_casos(engine, origen_schema, destino_schema):

    tabla_origen = 'data'
    tabla_destino = "hechos_casos"

    print(f"\n---  Iniciando ETL para '{tabla_destino}' ---")
    try:
        with engine.connect() as connection:
            # FASE DE EXTRACCIÓN
            print("1. Extrayendo datos...")
            
            
            columnas_necesarias = [
                '"direc_cod_depto_victima-2" as cnegociod',
                'cod_muni_hecho as cnegociom',
                'tipo_agresion_descripcion',
                'fh_caso',
                'dpi_victima'
            ]
            sql_h = f'SELECT {", ".join(columnas_necesarias)} FROM "{origen_schema}"."{tabla_origen}"'
            df_hechos = pd.read_sql(sql_h, connection)
            print(f"   -> Datos extraídos: {len(df_hechos)} registros.")
            
            # Extraer dimensiones para lookup
            df_dim_victima = pd.read_sql_table("dim_victima", connection, schema=destino_schema)
            df_dim_direccion = pd.read_sql_table("dim_direccion", connection, schema=destino_schema)
            df_dim_tiempo = pd.read_sql_table("dim_tiempo", connection, schema=destino_schema)
            df_dim_violencia = pd.read_sql_table("dim_tipo_violencia", connection, schema=destino_schema)
            print("   -> Dimensiones extraídas para lookup.")

            # FASE DE TRANSFORMACIÓN Y LOOKUPS
            print("\n2. Transformando datos y buscando claves de dimensión (Lookups)...")

            # Lookup de Dirección
            print("   - Buscando 'id_direccion'...")
            # Aseguramos que los tipos de datos de las claves coincidan
            for col in ['cnegociod', 'cnegociom']:
                df_hechos[col] = pd.to_numeric(df_hechos[col], errors='coerce').fillna(-1).astype(int) # Convertir a int
                df_dim_direccion[col] = pd.to_numeric(df_dim_direccion[col], errors='coerce').fillna(-1).astype(int) # Convertir a int
            
      
            df_lookup_dir = df_dim_direccion[['id_direccion', 'cnegociod', 'cnegociom']]
            df_hechos = pd.merge(df_hechos, df_lookup_dir, on=['cnegociod', 'cnegociom'], how='left')
            
            # Lookup de Víctima
            print("   - Buscando 'id_victima'...")
            
            # Convertir la columna de hechos (double precision) a un entero para truncar decimales.
            # Usamos 'Int64' (con mayúscula) de pandas porque puede manejar valores nulos (NaN).
            df_hechos['dpi_victima_int'] = pd.to_numeric(df_hechos['dpi_victima'], errors='coerce').astype('Int64')

            # Convertir la clave a string limpio para la unión. Esto evita problemas con floats.
            df_hechos['dpi_victima_limpio'] = df_hechos['dpi_victima_int'].astype(str).str.strip().replace('<NA>', '')

            # Asegurarse de que la clave de la dimensión (bigint) también se trate como string limpio.
            df_dim_victima['victima_f_original_limpio'] = df_dim_victima['victima_f_original'].astype(str).str.strip()
            
            
            df_lookup_vic = df_dim_victima[['id_victima', 'victima_f_original_limpio']]
            df_hechos = pd.merge(df_hechos, df_lookup_vic, left_on='dpi_victima_limpio', right_on='victima_f_original_limpio', how='left')
            # --- Lookup de Tipo de Violencia ---
            print("   - Buscando 'id_tipo_violencia'...")
            
            df_hechos['violencia_norm'] = df_hechos['tipo_agresion_descripcion'].str.strip().str.title()
            df_dim_violencia['nombre_violencia'] = df_dim_violencia['nombre_violencia'].str.strip().str.title()
            
            df_lookup_vio = df_dim_violencia[['id_violencia', 'nombre_violencia']]
            df_hechos = pd.merge(df_hechos, df_lookup_vio, left_on='violencia_norm', right_on='nombre_violencia', how='left')

            # Lookup de Tiempo
            print("   - Buscando 'id_tiempo'...")
            df_hechos['fecha_h'] = pd.to_datetime(df_hechos['fh_caso'], errors='coerce').dt.date
            df_dim_tiempo['fecha'] = pd.to_datetime(df_dim_tiempo['fecha']).dt.date
            
            df_lookup_tie = df_dim_tiempo[["id_tiempo", "fecha"]]
            df_hechos = pd.merge(df_hechos, df_lookup_tie, left_on='fecha_h', right_on='fecha', how='left')

            print("   -> Lookups completados.")
            
            # CÁLCULO DE MÉTRICAS
            print("\n3. Calculando métricas de negocio...")
            df_hechos['numero_casos'] = 1 # Métrica principal
            
            # Cálculo de reincidencia
            df_hechos_validos = df_hechos.dropna(subset=['id_victima', 'fecha_h']).copy()
            df_hechos_validos = df_hechos_validos.sort_values(by=['id_victima', 'fecha_h'])
            df_hechos_validos['reincidencia'] = df_hechos_validos.groupby('id_victima').cumcount()
            df_hechos['reincidencia_casos'] = (df_hechos_validos['reincidencia'] > 0).astype(int)
            df_hechos['reincidencia_casos'].fillna(0, inplace=True)
            print(f"   -> 'reincidencia_casos' calculada. Total: {int(df_hechos['reincidencia_casos'].sum())}")

            # PREPARACIÓN FINAL Y CARGA
            print("\n4. Limpiando y preparando para la carga...")
            columnas_finales = [
                'id_victima',
                'id_violencia',
                'id_direccion',
                'id_tiempo',
                'reincidencia_casos',
                'numero_casos'
            ]
            df_final = df_hechos[columnas_finales]

            # Reemplazar NaNs con un valor para 'Desconocido' (-1)
            valores_llenar = {col: -1 for col in columnas_finales}
            df_final = df_final.fillna(valores_llenar)

            # Convertir todas las columnas a enteros
            for col in df_final.columns:
                df_final[col] = df_final[col].astype(int)
            
            print(f"   -> Datos listos para cargar: {len(df_final)} registros.")

            # CARGA (Load)
            print("\n5. Cargando datos en la base de datos...")
            connection.execute(text(f'TRUNCATE TABLE "{destino_schema}"."{tabla_destino}";'))
            connection.commit()
            
            df_final.to_sql(tabla_destino, engine, schema=destino_schema, if_exists='append', index=False)
            
            print(f"   -> Carga finalizada. Se insertaron {len(df_final)} registros.")
            print(f"\n ETL para '{tabla_destino}' completado exitosamente.")

            return len(df_final) # Retorna el número de registros cargados

    except KeyError as e:
        print(f"ERROR de Clave: No se encontró la columna '{e}'. Revisa los nombres de las columnas en tus DataFrames.")
        raise
    except Exception as e:
        print(f"ERROR inesperado en el ETL de '{tabla_destino}': {e}")
        raise

def main():
    """
    Función principal que orquesta la conexión y la ejecución de todos los ETL.
    """
    load_dotenv()
    
    # Obtiene las credenciales de forma segura
    db_user = os.getenv("model_user_db")
    db_password = os.getenv("model_password_db")
    db_host = os.getenv("db_host")
    db_port = os.getenv("db_port")
    db_name = os.getenv("db_name")
    origen_schema = os.getenv("origen_schema")
    destino_schema = os.getenv("destino_schema")

    if not all([db_user, db_password, db_host, db_port, db_name, origen_schema, destino_schema]):
        print(" ERROR: Faltan variables de entorno en el archivo .env.")
        sys.exit(1)
      

    try:
            # Crear la cadena de conexión
            
        connection_string = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(connection_string)

        with engine.connect() as connection:
            
            print("Conexión a la base de datos establecida.")

            # Ejecutar los procesos ETL
        proceso_etl={"hechos_casos": etl_hechos_casos}
        for nombre, funcion_etl in proceso_etl.items():
            fecha=datetime.datetime.now(datetime.timezone.utc)
            try:
                registros=funcion_etl(engine, origen_schema, destino_schema)
                registrar_log(engine,  nombre, fecha, 'Exitoso', registros)
            except Exception as e:
                print(f"ERROR en  '{nombre}': {e}")
                
                registrar_log(engine,  nombre, fecha, 'Fallido', error=e)
                print(f" Proceso fatal en '{nombre}' .El proceso se detendrá.")
                sys.exit(1)
        print("\n El proceso ETL ha finalizado exitosamente.")
            
    except Exception as e:
        print(f" El proceso Etl se detubo debido a un error crítico: {e}")

        sys.exit(1)
    finally:
        if engine:
            engine.dispose()
            print("🔌 Conexión a la base de datos cerrada.")

if __name__ == "__main__":
    main()