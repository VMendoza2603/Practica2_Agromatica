import pandas as pd
from sqlalchemy import create_engine, text
import os

# 1. CONFIGURACIÓN DE LA BASE DE DATOS Y DEL ARCHIVO CSV
DB_USER = "postgres"
DB_PASS = "59371"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "clima_agro"

# Configuración del CSV
CSV_FILE_PATH = "datos_guayas.csv"
TABLE_NAME = "lecturas"

# El archivo tiene 10 líneas de metadatos antes del encabezado (YEAR,DOY,...)
SKIP_ROWS = 10
# Valor para datos faltantes (será tratado como NULL en PostgreSQL)
MISSING_DATA_VALUE = -999

# 2. DEFINICIÓN DE LA ESTRUCTURA DE LA TABLA (SQL)

# Creamos la tabla 'lecturas' con la estructura del CSV: YEAR, DOY, T2M, RH2M
SQL_CREATE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    YEAR INT,
    DOY INT,
    T2M NUMERIC,     -- Temperatura a 2 Metros (C)
    RH2M NUMERIC     -- Humedad Relativa a 2 Metros (%)
);
"""
# 3. LÓGICA DE IMPORTACIÓN Y TRANSFORMACIÓN

def importar_csv_a_postgres():
    """Lee el CSV, limpia los datos y los guarda en PostgreSQL."""
    print(f"Iniciando la importación a la tabla: {TABLE_NAME}")

    # 3.1. Crear la conexión a la base de datos (Engine)
    try:
        # Cadena de conexión (format: postgresql://user:pass@host:port/dbname)
        db_url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(db_url)
        print("Conexión con PostgreSQL establecida.")
    except Exception as e:
        print(f"ERROR al conectar con PostgreSQL. ¿Están tus credenciales correctas? Detalle: {e}")
        return

    # 3.2. Crear la tabla 'lecturas' si no existe
    try:
        with engine.connect() as connection:
            connection.execute(text(SQL_CREATE_TABLE))
            connection.commit()
        print(f"Tabla '{TABLE_NAME}' verificada/creada en '{DB_NAME}'.")
    except Exception as e:
        print(f"ERROR al crear la tabla: {e}")
        return

    # 3.3. Lectura y Limpieza del CSV con Pandas
    try:
        df = pd.read_csv(
            CSV_FILE_PATH,
            skiprows=SKIP_ROWS,
            na_values=[MISSING_DATA_VALUE]
        )
        # Limpieza de nombres de columna:
        df.columns = [col.strip() for col in df.columns]

        #CONVERTIR NOMBRES A MINÚSCULAS ===
        df.columns = [col.lower() for col in df.columns]

        print(f"CSV leído y limpiado. Filas para importar: {len(df)}")

    except FileNotFoundError:
        print(f"ERROR: El archivo CSV no se encontró. Asegúrate de que '{CSV_FILE_PATH}' esté en la misma carpeta que el script.")
        return
    except Exception as e:
        print(f"ERROR al leer o procesar el CSV: {e}")
        return

    # 3.4. Carga de datos a PostgreSQL
    try:
        # Carga el DataFrame a la tabla 'lecturas'
        df.to_sql(
            TABLE_NAME,
            engine,
            if_exists='append',
            index=False,
            chunksize=1000
        )
        print("\n=======================================================")
        print(f"¡ÉXITO! {len(df)} filas importadas a la tabla '{TABLE_NAME}'.")
        print("=======================================================")

    except Exception as e:
        print(f"\n ERROR al insertar datos en la base de datos: {e}")

if __name__ == "__main__":
    importar_csv_a_postgres()



