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
table_id = "liquid-optics-447914-a9.ENCAJESTEST.FACT_VDETALLECOS3"

# Tabla temporal en BigQuery
temp_table_id = f"{table_id}_temp"

try:
    # Consulta a Oracle
    query = """ select 
NOMCLIENTE,
PIEZAPP,
NOMBRELINEACCIAL,
REFEF4111,
CLASIFVENTA,
TIPOVENTAF,
ANOCOS2,
MESCOS2,
RN,
DOCFACT,
TIPOFAC,
NOMDIVIS,
PIEZATOT,
NOMCOSTOTO,
PIEZATOTO,
TOTALCOM,
IGCOSTTO,
COSTOMT,
COSTOVENTA,
MTSP,
KILOSPROD,
CANTV,
TOTALCOSVENTA
 from analitica.fact_vdetallecos3
    """
    cursor = conn.cursor()
    cursor.execute(query)

    # Obtén los nombres de las columnas
    column_names = [col[0] for col in cursor.description]
    df = pd.DataFrame([dict(zip(column_names, row)) for row in cursor.fetchall()])

    logging.info("Consulta a Oracle ejecutada exitosamente")
    print(df)
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
        ON destino.NOMCLIENTE = origen.NOMCLIENTE  -- Ajusta la clave primaria según tu tabla

        WHEN MATCHED THEN 
          UPDATE SET 
    destino.PIEZAPP = origen.PIEZAPP,
    destino.NOMBRELINEACCIAL = origen.NOMBRELINEACCIAL,
    destino.REFEF4111 = origen.REFEF4111,
    destino.CLASIFVENTA = origen.CLASIFVENTA,
    destino.TIPOVENTAF = origen.TIPOVENTAF,
    destino.ANOCOS2 = origen.ANOCOS2,
    destino.MESCOS2 = origen.MESCOS2,
    destino.RN = origen.RN,
    destino.DOCFACT = origen.DOCFACT,
    destino.TIPOFAC = origen.TIPOFAC,
    destino.NOMDIVIS = origen.NOMDIVIS,
    destino.PIEZATOT = origen.PIEZATOT,
    destino.NOMCOSTOTO = origen.NOMCOSTOTO,
    destino.PIEZATOTO = origen.PIEZATOTO,
    destino.TOTALCOM = origen.TOTALCOM,
    destino.IGCOSTTO = origen.IGCOSTTO,
    destino.COSTOMT = origen.COSTOMT,
    destino.COSTOVENTA = origen.COSTOVENTA,
    destino.MTSP = origen.MTSP,
    destino.KILOSPROD = origen.KILOSPROD,
    destino.CANTV = origen.CANTV,
    destino.TOTALCOSVENTA = origen.TOTALCOSVENTA
        WHEN NOT MATCHED THEN 
          INSERT (NOMCLIENTE, PIEZAPP, NOMBRELINEACCIAL, REFEF4111, CLASIFVENTA, TIPOVENTAF, ANOCOS2, MESCOS2, RN, DOCFACT, TIPOFAC, NOMDIVIS, PIEZATOT, NOMCOSTOTO, PIEZATOTO, TOTALCOM, IGCOSTTO, COSTOMT, COSTOVENTA, MTSP, KILOSPROD, CANTV, TOTALCOSVENTA
)
          VALUES (origen.NOMCLIENTE, origen.PIEZAPP, origen.NOMBRELINEACCIAL, origen.REFEF4111, origen.CLASIFVENTA, origen.TIPOVENTAF, origen.ANOCOS2, origen.MESCOS2, origen.RN, origen.DOCFACT, origen.TIPOFAC, origen.NOMDIVIS, origen.PIEZATOT, origen.NOMCOSTOTO, origen.PIEZATOTO, origen.TOTALCOM, origen.IGCOSTTO, origen.COSTOMT, origen.COSTOVENTA, origen.MTSP, origen.KILOSPROD, origen.CANTV, origen.TOTALCOSVENTA
);
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
