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
table_id = "liquid-optics-447914-a9.ENCAJESTEST.FACT_VCOSTOS4"

# Tabla temporal en BigQuery
temp_table_id = f"{table_id}_temp"

try:
    # Consulta a Oracle
    query = """ select 
NOMCLIENTE,
RNT,
NOMBRELINEACCIAL,
DOCFACT,
PIEZAPP,
PIEZATOT,
REFEF4111,
TIPOVENTAF,
CLASIFVENTA,
ANOCOS2,
MESCOS2,
IGCOSTTO,
NOMDIVIS,
NOMCOSTOTO,
VALCOSTO,
TOTVENTADIVISION,
VENTAMES,
VENTAANO,
VENTADIV,
TOTVENTAPIEZA,
VENTAPIEZA,
TOTCOMISION,
TOTFLETES,
VARIACIONMES,
VALCOMISION,
VALFLETES,
A1MATERIALES,
A2DESECHOS,
B1MOD,
B3ENERGIA,
C1REPUESTOS,
C2DEPRECIACION,
C4SERVICIOS,
VAUORG,
VARIAA1,
VARIAA2,
VARIAB1,
VARIAB3,
VARIAC1,
VARIAC2,
VARIAC4,
COSTOA1CVA,
COSTOA2CVA,
COSTOB1CVA,
COSTOB3CVA,
COSTOC1CVA,
COSTOC2CVA,
COSTOC4CVA,
TOTALCOSCONVAR,
VALPROGASTOSADM,
VALPROGASTOSVEN,
VALPROGASTOSFIN,
ANOPRODUC,
TVENTAPRODU,
TOTALCOSVARIABLES,
TOTALCOSFIJOS,
GASTOSADM,
GASTOVENTA,
GASTOSFINA
 from analitica.fact_vcostos4
    """
    cursor = conn.cursor()
    cursor.execute(query)

    # Obtén los nombres de las columnas
    column_names = [col[0] for col in cursor.description]
    df = pd.DataFrame([dict(zip(column_names, row)) for row in cursor.fetchall()])

    logging.info("Consulta a Oracle ejecutada exitosamente")
    df["ANOPRODUC"] = df["ANOPRODUC"].astype(pd.Int64Dtype())  # Convierte a entero sin decimales
    df["VAUORG"] = df["VAUORG"].astype(pd.Int64Dtype())  # Convierte a entero sin decimales
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
destino.RNT = origen.RNT,
destino.NOMBRELINEACCIAL = origen.NOMBRELINEACCIAL,
destino.DOCFACT = origen.DOCFACT,
destino.PIEZAPP = origen.PIEZAPP,
destino.PIEZATOT = origen.PIEZATOT,
destino.REFEF4111 = origen.REFEF4111,
destino.TIPOVENTAF = origen.TIPOVENTAF,
destino.CLASIFVENTA = origen.CLASIFVENTA,
destino.ANOCOS2 = origen.ANOCOS2,
destino.MESCOS2 = origen.MESCOS2,
destino.IGCOSTTO = origen.IGCOSTTO,
destino.NOMDIVIS = origen.NOMDIVIS,
destino.NOMCOSTOTO = origen.NOMCOSTOTO,
destino.VALCOSTO = origen.VALCOSTO,
destino.TOTVENTADIVISION = origen.TOTVENTADIVISION,
destino.VENTAMES = origen.VENTAMES,
destino.VENTAANO = origen.VENTAANO,
destino.VENTADIV = origen.VENTADIV,
destino.TOTVENTAPIEZA = origen.TOTVENTAPIEZA,
destino.VENTAPIEZA = origen.VENTAPIEZA,
destino.TOTCOMISION = origen.TOTCOMISION,
destino.TOTFLETES = origen.TOTFLETES,
destino.VARIACIONMES = origen.VARIACIONMES,
destino.VALCOMISION = origen.VALCOMISION,
destino.VALFLETES = origen.VALFLETES,
destino.A1MATERIALES = origen.A1MATERIALES,
destino.A2DESECHOS = origen.A2DESECHOS,
destino.B1MOD = origen.B1MOD,
destino.B3ENERGIA = origen.B3ENERGIA,
destino.C1REPUESTOS = origen.C1REPUESTOS,
destino.C2DEPRECIACION = origen.C2DEPRECIACION,
destino.C4SERVICIOS = origen.C4SERVICIOS,
destino.VAUORG = origen.VAUORG,
destino.VARIAA1 = origen.VARIAA1,
destino.VARIAA2 = origen.VARIAA2,
destino.VARIAB1 = origen.VARIAB1,
destino.VARIAB3 = origen.VARIAB3,
destino.VARIAC1 = origen.VARIAC1,
destino.VARIAC2 = origen.VARIAC2,
destino.VARIAC4 = origen.VARIAC4,
destino.COSTOA1CVA = origen.COSTOA1CVA,
destino.COSTOA2CVA = origen.COSTOA2CVA,
destino.COSTOB1CVA = origen.COSTOB1CVA,
destino.COSTOB3CVA = origen.COSTOB3CVA,
destino.COSTOC1CVA = origen.COSTOC1CVA,
destino.COSTOC2CVA = origen.COSTOC2CVA,
destino.COSTOC4CVA = origen.COSTOC4CVA,
destino.TOTALCOSCONVAR = origen.TOTALCOSCONVAR,
destino.VALPROGASTOSADM = origen.VALPROGASTOSADM,
destino.VALPROGASTOSVEN = origen.VALPROGASTOSVEN,
destino.VALPROGASTOSFIN = origen.VALPROGASTOSFIN,
destino.ANOPRODUC = origen.ANOPRODUC,
destino.TVENTAPRODU = origen.TVENTAPRODU,
destino.TOTALCOSVARIABLES = origen.TOTALCOSVARIABLES,
destino.TOTALCOSFIJOS = origen.TOTALCOSFIJOS,
destino.GASTOSADM = origen.GASTOSADM,
destino.GASTOVENTA = origen.GASTOVENTA,
destino.GASTOSFINA = origen.GASTOSFINA
        WHEN NOT MATCHED THEN 
          INSERT (NOMCLIENTE,
RNT,
NOMBRELINEACCIAL,
DOCFACT,
PIEZAPP,
PIEZATOT,
REFEF4111,
TIPOVENTAF,
CLASIFVENTA,
ANOCOS2,
MESCOS2,
IGCOSTTO,
NOMDIVIS,
NOMCOSTOTO,
VALCOSTO,
TOTVENTADIVISION,
VENTAMES,
VENTAANO,
VENTADIV,
TOTVENTAPIEZA,
VENTAPIEZA,
TOTCOMISION,
TOTFLETES,
VARIACIONMES,
VALCOMISION,
VALFLETES,
A1MATERIALES,
A2DESECHOS,
B1MOD,
B3ENERGIA,
C1REPUESTOS,
C2DEPRECIACION,
C4SERVICIOS,
VAUORG,
VARIAA1,
VARIAA2,
VARIAB1,
VARIAB3,
VARIAC1,
VARIAC2,
VARIAC4,
COSTOA1CVA,
COSTOA2CVA,
COSTOB1CVA,
COSTOB3CVA,
COSTOC1CVA,
COSTOC2CVA,
COSTOC4CVA,
TOTALCOSCONVAR,
VALPROGASTOSADM,
VALPROGASTOSVEN,
VALPROGASTOSFIN,
ANOPRODUC,
TVENTAPRODU,
TOTALCOSVARIABLES,
TOTALCOSFIJOS,
GASTOSADM,
GASTOVENTA,
GASTOSFINA)
          VALUES (origen.NOMCLIENTE,
origen.RNT,
origen.NOMBRELINEACCIAL,
origen.DOCFACT,
origen.PIEZAPP,
origen.PIEZATOT,
origen.REFEF4111,
origen.TIPOVENTAF,
origen.CLASIFVENTA,
origen.ANOCOS2,
origen.MESCOS2,
origen.IGCOSTTO,
origen.NOMDIVIS,
origen.NOMCOSTOTO,
origen.VALCOSTO,
origen.TOTVENTADIVISION,
origen.VENTAMES,
origen.VENTAANO,
origen.VENTADIV,
origen.TOTVENTAPIEZA,
origen.VENTAPIEZA,
origen.TOTCOMISION,
origen.TOTFLETES,
origen.VARIACIONMES,
origen.VALCOMISION,
origen.VALFLETES,
origen.A1MATERIALES,
origen.A2DESECHOS,
origen.B1MOD,
origen.B3ENERGIA,
origen.C1REPUESTOS,
origen.C2DEPRECIACION,
origen.C4SERVICIOS,
origen.VAUORG,
origen.VARIAA1,
origen.VARIAA2,
origen.VARIAB1,
origen.VARIAB3,
origen.VARIAC1,
origen.VARIAC2,
origen.VARIAC4,
origen.COSTOA1CVA,
origen.COSTOA2CVA,
origen.COSTOB1CVA,
origen.COSTOB3CVA,
origen.COSTOC1CVA,
origen.COSTOC2CVA,
origen.COSTOC4CVA,
origen.TOTALCOSCONVAR,
origen.VALPROGASTOSADM,
origen.VALPROGASTOSVEN,
origen.VALPROGASTOSFIN,
origen.ANOPRODUC,
origen.TVENTAPRODU,
origen.TOTALCOSVARIABLES,
origen.TOTALCOSFIJOS,
origen.GASTOSADM,
origen.GASTOVENTA,
origen.GASTOSFINA
);"""
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
