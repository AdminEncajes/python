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
table_id = "liquid-optics-447914-a9.ENCAJESTEST.FACT_VDETALLECOS1"

# Tabla temporal en BigQuery
temp_table_id = f"{table_id}_temp"

try:
    # Consulta a Oracle
    query = """
SELECT TIPOVENTAF,
CLASIFVENTA,
TIPOPIEZA,
CIAF411,
CODREFF4111,
CODLARF4111,
REFEF4111,
NUMDOCF4111,
TIPDOCF4111,
PIEZAF4111,
PIEZAMADRE,
CANTVENDF4111,
UNDF4111,
COSTOKA,
TOTALVENTAF4111,
VENDEDORF4111,
NOMCLIENTEF4111,
TO_CHAR(FECHAT, 'YYYY-MM-DD') AS FECHAT,
NOMBRELINEACCIAL,
NOMBREDIVISION,
UNIDADVENTA,
MESF4111,
ANOF411,
RN
FROM ANALITICA.FACT_VDETALLECOS1
    """
    cursor = conn.cursor()
    cursor.execute(query)

    # Obtén los nombres de las columnas
    column_names = [col[0] for col in cursor.description]
    df = pd.DataFrame([dict(zip(column_names, row)) for row in cursor.fetchall()])

    logging.info("Consulta a Oracle ejecutada exitosamente")
    print(df)
    df['FECHAT'] = pd.to_datetime(df['FECHAT'], errors='coerce').dt.date
    if not df.empty:
        # Cargar los datos en una tabla temporal en BigQuery
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(df, temp_table_id, job_config=job_config)
        job.result()  # Espera a que termine la carga

        logging.info("Datos cargados en la tabla temporal de BigQuery")

        # Consulta MERGE en BigQuery
        merge_query = f"""
        MERGE `{table_id}` AS destino
        USING `{temp_table_id}` AS origen
        ON destino.TIPOVENTAF = origen.TIPOVENTAF  -- Ajusta la clave primaria según tu tabla

        WHEN MATCHED THEN 
          UPDATE SET 

destino.CLASIFVENTA = origen.CLASIFVENTA,
destino.TIPOPIEZA = origen.TIPOPIEZA,
destino.CIAF411 = origen.CIAF411,
destino.CODREFF4111 = origen.CODREFF4111,
destino.CODLARF4111 = origen.CODLARF4111,
destino.REFEF4111 = origen.REFEF4111,
destino.NUMDOCF4111 = origen.NUMDOCF4111,
destino.TIPDOCF4111 = origen.TIPDOCF4111,
destino.PIEZAF4111 = origen.PIEZAF4111,
destino.PIEZAMADRE = origen.PIEZAMADRE,
destino.CANTVENDF4111 = origen.CANTVENDF4111,
destino.UNDF4111 = origen.UNDF4111,
destino.COSTOKA = origen.COSTOKA,
destino.TOTALVENTAF4111 = origen.TOTALVENTAF4111,
destino.VENDEDORF4111 = origen.VENDEDORF4111,
destino.NOMCLIENTEF4111 = origen.NOMCLIENTEF4111,
destino.FECHAT = origen.FECHAT,
destino.NOMBRELINEACCIAL = origen.NOMBRELINEACCIAL,
destino.NOMBREDIVISION = origen.NOMBREDIVISION,
destino.UNIDADVENTA = origen.UNIDADVENTA,
destino.MESF4111 = origen.MESF4111,
destino.ANOF411 = origen.ANOF411,
destino.RN = origen.RN

        WHEN NOT MATCHED THEN 
          INSERT (TIPOVENTAF,CLASIFVENTA,TIPOPIEZA,CIAF411,CODREFF4111,CODLARF4111,REFEF4111,NUMDOCF4111,TIPDOCF4111,PIEZAF4111,PIEZAMADRE,CANTVENDF4111,UNDF4111,
COSTOKA,TOTALVENTAF4111,VENDEDORF4111,NOMCLIENTEF4111,FECHAT,NOMBRELINEACCIAL,NOMBREDIVISION,UNIDADVENTA,MESF4111,ANOF411,RN)
          VALUES (origen.TIPOVENTAF,origen.CLASIFVENTA,origen.TIPOPIEZA,origen.CIAF411,origen.CODREFF4111,origen.CODLARF4111,origen.REFEF4111,origen.NUMDOCF4111,origen.TIPDOCF4111,origen.PIEZAF4111,origen.PIEZAMADRE,origen.CANTVENDF4111,origen.UNDF4111,origen.COSTOKA,origen.TOTALVENTAF4111,origen.VENDEDORF4111,origen.NOMCLIENTEF4111,origen.FECHAT,origen.NOMBRELINEACCIAL,origen.NOMBREDIVISION,origen.UNIDADVENTA,origen.MESF4111,origen.ANOF411,origen.RN);
        """

        # Ejecutar el MERGE en BigQuery
        merge_job = client.query(merge_query)
        merge_job.result()  # Espera a que termine

        logging.info("MERGE completado con éxito en BigQuery")

    else:
        logging.warning("El DataFrame está vacío, no se realizaron cambios en BigQuery")

except Exception as e:
    logging.error(f"Error durante la ejecución: {e}", exc_info=True)

finally:
    if conn:
        conn.close()
        logging.info("Conexión a Oracle cerrada")
