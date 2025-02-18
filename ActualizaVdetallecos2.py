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
table_id = "liquid-optics-447914-a9.ENCAJESTEST.FACT_VDETALLECOS2"

# Tabla temporal en BigQuery
temp_table_id = f"{table_id}_temp"

try:
    # Consulta a Oracle
    query = """ select 
PIEZAPP,
RN,
IGITM,
IGCOST,
IGDOCOV2,
PIEZAV2,
MTSPROD,
SEGF1,
NOMORDEN,
NONORDEYNUM,
TIPDOCF4111,
NOMBRELINEACCIAL,
NUMDOCF4111,
MESF4111,
ANOF411,
NOMCLIENTEF4111,
CANTVENDF4111,
VA1,
VA2,
VB1,
VB3,
VC1,
VC2,
VC4 from analitica.fact_vdetallecos2
    """
    cursor = conn.cursor()
    cursor.execute(query)

    # Obtén los nombres de las columnas
    column_names = [col[0] for col in cursor.description]
    df = pd.DataFrame([dict(zip(column_names, row)) for row in cursor.fetchall()])
    logging.info("Consulta a Oracle ejecutada exitosamente")
    df["IGITM"] = df["IGITM"].astype(pd.Int64Dtype())  # Convierte a entero sin decimales
    df["IGDOCOV2"] = df["IGDOCOV2"].astype(pd.Int64Dtype())  # Convierte a entero sin decimales
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
        ON destino.PIEZAPP = origen.PIEZAPP  -- Ajusta la clave primaria según tu tabla

        WHEN MATCHED THEN 
          UPDATE SET 
destino.RN = origen.RN,
destino.IGITM = origen.IGITM,
destino.IGCOST = origen.IGCOST,
destino.IGDOCOV2 = origen.IGDOCOV2,
destino.PIEZAV2 = origen.PIEZAV2,
destino.MTSPROD = origen.MTSPROD,
destino.SEGF1 = origen.SEGF1,
destino.NOMORDEN = origen.NOMORDEN,
destino.NONORDEYNUM = origen.NONORDEYNUM,
destino.TIPDOCF4111 = origen.TIPDOCF4111,
destino.NOMBRELINEACCIAL = origen.NOMBRELINEACCIAL,
destino.NUMDOCF4111 = origen.NUMDOCF4111,
destino.MESF4111 = origen.MESF4111,
destino.ANOF411 = origen.ANOF411,
destino.NOMCLIENTEF4111 = origen.NOMCLIENTEF4111,
destino.CANTVENDF4111 = origen.CANTVENDF4111,
destino.VA1 = origen.VA1,
destino.VA2 = origen.VA2,
destino.VB1 = origen.VB1,
destino.VB3 = origen.VB3,
destino.VC1 = origen.VC1,
destino.VC2 = origen.VC2,
destino.VC4 = origen.VC4
        WHEN NOT MATCHED THEN 
          INSERT (PIEZAPP,RN,IGITM,IGCOST,IGDOCOV2,PIEZAV2,MTSPROD,SEGF1,NOMORDEN,NONORDEYNUM,TIPDOCF4111,NOMBRELINEACCIAL,NUMDOCF4111,MESF4111,ANOF411,NOMCLIENTEF4111,CANTVENDF4111,
VA1,VA2,VB1,VB3,VC1,VC2,VC4)
          VALUES (origen.PIEZAPP, origen.RN, origen.IGITM, origen.IGCOST, origen.IGDOCOV2, origen.PIEZAV2, origen.MTSPROD, origen.SEGF1, origen.NOMORDEN, origen.NONORDEYNUM, origen.TIPDOCF4111, origen.NOMBRELINEACCIAL, origen.NUMDOCF4111, origen.MESF4111, origen.ANOF411, origen.NOMCLIENTEF4111, origen.CANTVENDF4111, origen.VA1, origen.VA2, origen.VB1, origen.VB3, origen.VC1, origen.VC2, origen.VC4
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
