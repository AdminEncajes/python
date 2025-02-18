import cx_Oracle
import pandas as pd
import logging
from google.cloud import bigquery
import os

# Configura logging para capturar errores
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Imprime la variable de entorno de credenciales de BigQuery
print("GOOGLE_APPLICATION_CREDENTIALS:", os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

# Conexión a Oracle
dsn_tns = cx_Oracle.makedsn(
    '192.168.27.2', '1521',
    service_name='pdbjdepro.encajesclouddb.encajescloudred.oraclevcn.com'
)
conn = cx_Oracle.connect(user='ANALITICA', password='analitica142857', dsn=dsn_tns)

# Cliente de BigQuery
client = bigquery.Client(project="liquid-optics-447914-a9")

# Tabla de destino en BigQuery
table_id = "liquid-optics-447914-a9.ENCAJESTEST.VFLUJOCAJA"

# Tabla para los datos de Oracle (permanente)
oracle_table_id = "liquid-optics-447914-a9.ENCAJESTEST.VFLUJOCAJA_ORACLE"  # Nombre para la tabla permanente

# Vista materializada
materialized_view_id = "liquid-optics-447914-a9.ENCAJESTEST.VFLUJOCAJA"

try:
    # Consulta a Oracle
    query = """
    SELECT CODPV, CLAFPROV, NOMPROV, MESRE, ANORE, TIPOG, VALCOP, VALUSD, TIPOPROC
    FROM analitica.vflujocaja 
    """
    cursor = conn.cursor()
    cursor.execute(query)

    # Obtén los nombres de las columnas
    column_names = [col[0] for col in cursor.description]
    df = pd.DataFrame([dict(zip(column_names, row)) for row in cursor.fetchall()])

    logging.info("Consulta a Oracle ejecutada exitosamente")
    print(df)

    if not df.empty:
        # Conversión a DATE en Pandas
        print(df.dtypes)  # Verifica los tipos: deben ser 'object'

        # Cargar los datos en la tabla PERMANENTE (no temporal)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")  # Usa WRITE_TRUNCATE para reemplazar los datos cada vez
        job = client.load_table_from_dataframe(df, oracle_table_id, job_config=job_config) # Carga a la tabla permanente
        job.result()

        logging.info("Datos cargados en la tabla permanente de BigQuery")

        # Crear o reemplazar la vista materializada (se actualiza automáticamente cuando cambia la tabla base)
        create_mv_query = f"""
        CREATE OR REPLACE MATERIALIZED VIEW `{materialized_view_id}` AS
        SELECT *
        FROM `{oracle_table_id}`
        """
        create_mv_job = client.query(create_mv_query)
        create_mv_job.result()

        logging.info("Vista materializada creada o reemplazada exitosamente")

    else:
        logging.warning("El DataFrame está vacío, no se realizaron cambios en BigQuery")

except Exception as e:
    logging.error(f"Error durante la ejecución: {e}", exc_info=True)

finally:
    if conn:
        conn.close()
        logging.info("Conexión a Oracle cerrada")

