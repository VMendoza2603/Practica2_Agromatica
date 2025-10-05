import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# 1. CONFIGURACIÓN DE LA BASE DE DATOS
DB_USER = "postgres"
DB_PASS = "59371"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "clima_agro"
TABLE_NAME = "lecturas"

# Cadena de conexión
db_url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)

# 2. ACTIVIDAD 1: Lectura, Head y Describe

print("--- 1. LECTURA DE DATOS DESDE POSTGRESQL ---")

try:
    sql_query = f"SELECT * FROM {TABLE_NAME} ORDER BY year, doy;"

    # Leer los datos directamente a un DataFrame de Pandas
    df = pd.read_sql(sql_query, engine)

    df['t2m'] = pd.to_numeric(df['t2m'], errors='coerce')
    df['rh2m'] = pd.to_numeric(df['rh2m'], errors='coerce')

    # Eliminar las filas donde los valores sean -999 (convertidos a NaN) para el análisis
    df = df[df['t2m'].notna() & (df['t2m'] != -999)]

except Exception as e:
    print(f"ERROR al leer la base de datos: {e}")
    exit()

# Mostrar los primeros registros
print("\n--- 2. df.head() (Primeros Registros) ---")
print(df.head())

# Obtener y mostrar estadísticas
print("\n--- 3. df.describe() (Estadísticas Descriptivas) ---")
print(df[['t2m', 'rh2m']].describe())

# donde RH2M es igual a su valor máximo
dia_max_humedad = df[df['rh2m'] == df['rh2m'].max()]

# Imprimir el resultado
print(dia_max_humedad[['doy', 'rh2m']])
# 3. ACTIVIDAD 2: Crear un Gráfico de Líneas

# Crear una columna de fecha combinando YEAR y DOY (Día del Año)
# Esto es necesario para una correcta visualización en el eje X.
df['Fecha'] = pd.to_datetime(df['year'].astype(str) + df['doy'].astype(str), format='%Y%j')

plt.style.use('seaborn-v0_8-darkgrid')
fig, ax1 = plt.subplots(figsize=(12, 6))
plt.title(f'Tendencia Diaria de Temperatura y Humedad ({df["Fecha"].min().year})', fontsize=16)

ax1.set_xlabel('Fecha (Día del Año)')
color = 'tab:red'
ax1.set_ylabel('Temperatura (°C)', color=color)
ax1.plot(df['Fecha'], df['t2m'], color=color, marker='o', linestyle='-', label='Temperatura (T2M)')
ax1.tick_params(axis='y', labelcolor=color)

ax2 = ax1.twinx()
color = 'tab:blue'
ax2.set_ylabel('Humedad Relativa (%)', color=color)
ax2.plot(df['Fecha'], df['rh2m'], color=color, marker='x', linestyle='--', label='Humedad (RH2M)')
ax2.tick_params(axis='y', labelcolor=color)
# Formatear el eje X para mostrar fechas claramente
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
ax1.figure.autofmt_xdate()
# Mostrar la leyenda (combina las dos líneas)
fig.legend(loc="upper right", bbox_to_anchor=(1, 1), bbox_transform=ax1.transAxes)
plt.show()