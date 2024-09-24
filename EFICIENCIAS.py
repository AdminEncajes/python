# IMPORTA LIBRERIAS
import pyodbc
import pandas as pd
import cx_Oracle
import logging
import datetime

# Define Ruta de LOG
logging.basicConfig(filename='/home/ubuntu/logs/EFICIENCIAS.log', level=logging.
INFO)
# Obtiene Info Fecha
fecha = datetime.datetime.now()

# VARIABLES CONEXION SQL SERVER
server = '192.169.1.25'
bd = 'MAESTRO'
user = 'BIEncajes'
password = 'EncaBI2023$$'

# CONECTA A LA BASE DE DATOS ORACLE
dsn_tns = cx_Oracle.makedsn('192.168.27.2', '1521', service_name='pdbjdepro.enca
jesclouddb.encajescloudred.oraclevcn.com')
conn = cx_Oracle.connect(user=r'ANALITICA', password='analitica142857', dsn=dsn_
tns)

# CONECTA A LA BASE DE DATOS DE SQL SERVER TEJEDURIA
try:
    conexion = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL server};SERVER='+s
erver+';DATABASE='+bd+';UID='+user+';PWD='+password)
    logging.info(fecha)
    logging.info('Conexion exitosa')

    # EJECUTA LA SENTENCIA CONTRA SERVIDOR TEJEDURIA
    query = """
    DECLARE @FechaAyer DATE
    SET @FechaAyer = DATEADD(day, -1, GETDATE())
    SELECT
    IDCONEXION, 
    IDMAQUINA, 
    CALCULO2 AS EficOper, 
    CALCULO1 AS EficReal, 
    CONCAT(CONCAT(FORMAT(@FechaAyer, 'MMMM'), '-'), DAY(@FechaAyer)) AS FechaFor
,
    CALCULO3, 
    CALCULO4, 
    CALCULO5, 
    CONCAT(
        DAY(@FechaAyer), '-',
        SUBSTRING(
            REPLACE(REPLACE(REPLACE(REPLACE(UPPER(FORMAT(@FechaAyer, 'MMM')), 'A
BR', 'APR'), 'ENE', 'JAN'), 'DIC', 'DEC'), 'AGO', 'AUG'), 1, 3),
        '-', RIGHT(FORMAT(@FechaAyer, 'yy'), 2)
    ) AS FECHA
FROM 
    MAESTRO.dbo.FMAQOEE
WHERE 
    IDCONEXION = CONCAT('65', CONVERT(VARCHAR(6), RIGHT(CONVERT(VARCHAR(8), @Fec
haAyer, 112), 6)))
    AND CALCULO1 IS NOT NULL
    AND CALCULO2 IS NOT NULL
    AND CALCULO3 IS NOT NULL
    AND CALCULO4 IS NOT NULL
    AND CALCULO5 IS NOT NULL
ORDER BY 
    DTREGISTRO, IDMAQUINA DESC
    """

    cursor = conexion.cursor()
    cursor.execute(query)

    # OBTIENE LOS NOMBRES DE LAS COLUMNAS
    column_names = [col[0] for col in cursor.description]

    # Almacenar tados del select en Dataframe con nombres de columna
    df = pd.DataFrame([dict(zip(column_names, row)) for row in cursor.fetchall()
])
    print(df)

    logging.info('Consulta ejecutada exitosamente')

    # Insertar datos del DF en la base de datos Oracle
    for index, row in df.iterrows():
        row_dict = dict(row)
        oracle_cursor = conn.cursor()  # Create a cursor for Oracle connection
        oracle_cursor.execute(
            """INSERT INTO ANALITICA.FACT_EFICIENCIAS (ID,IDCONEXION, IDMAQUINA,
 EFICOPER, EFICREAL, FECHAFOR, CALCULO3, CALCULO4, CALCULO5, FECHA)
               VALUES (EFICIE_SEQ.NEXTVAL,:IDCONEXION, :IDMAQUINA, :EFICOPER, :E
FICREAL, :FECHAFOR, :CALCULO3, :CALCULO4, :CALCULO5, :FECHA) """, row_dict)
        conn.commit()  # ENVIA CAMBIOS A LA BD

except Exception as e:
    logging.info(e)

finally:
    # CIERRA LAS CONEXIONES A LA BD
    if conexion:
        conexion.close()
        logging.info('Insercion Exitosa, conexion Infotint Cerrada')
    if conn:
        conn.close()
        logging.info('Conexion Oracle Cerrada')
