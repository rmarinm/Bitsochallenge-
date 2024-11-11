import psycopg2
import pandas as pd
from config import DWH_DB_HOST, DWH_DB_NAME, DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB_SCHEMA

# Conexión al DWH
conn = psycopg2.connect(
    host=DWH_DB_HOST,
    database=DWH_DB_NAME,
    user=DWH_DB_USER,
    password=DWH_DB_PASSWORD,
    options=f'-c search_path={DWH_DB_SCHEMA}'
)

# Función para exportar tablas a CSV
def export_table_to_csv(table_name):
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, conn)
    df.to_csv(f"{table_name}.csv", index=False)
    print(f"Datos exportados de {table_name} a {table_name}.csv")

# Lista de tablas a exportar
tables = ["Dim_User", "Dim_Jurisdiction", "Dim_UserLevel", "Dim_Product", "Dim_Date", "Fact_Transactions", "Fact_Events", "Fact_Activities"]

for table in tables:
    export_table_to_csv(table)

# Cerrar conexión
conn.close()
print("Exportación a CSV completa.")
