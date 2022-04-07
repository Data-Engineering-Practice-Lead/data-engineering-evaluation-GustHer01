#### Para el proyecto utilizo s3 de aws, creo un bucket en s3
#### las credenciales del bucket deben de ir en el archivo credentials.py
#### se necesita access key, secret key y nombre del bucket

### Ejercicio 1
1. extract_from_api.py realiza lo siguiente:
    - extract_json recibe una url de la api y extrae el json de respuesta, en python lo recibo como un diccionario
    - get_states recibe un json extraido con la función extract_json, obtiene los state codes de cada estado y genera urls para extraer la información por estado del api. Con esas urls extrae los jsons(diccionarios en python) de cada estado y escribe los dicctionarios como archivos .json en la carpeta de nombre data
    - get_dates extrae las fechas del json de la api para generar el calendario que nos piden, extrae una fecha de inicio y una de fin
    - gen_calendar genera un rango de fechas con las fechas extraidas en la función get_dates, este rango de fechas será convertido mas adelante en el calendario.
    - s3_connection establece la conexión con un bucket de s3
    - write_to_s3 sube todo lo que esté en la carpeta data a el bucket de s3

    - los archivos covid_states.json, covid_transactions.json y calendar.json son tambien convertidos a archivos .parquet segun los requerimientos
    - estos archivos tambien se guardan en la carpeta data y tambien son subidos a s3

2. Para lo que se pide de staging utilicé snowflake como datawarehouse
eso lo hacen los archivos staging_calendar.sql, staging_states_snowflake.sql y staging_transactions_snowflake.sql, desde s3 cargan al staging area de snowflake las 3
tablas principales que son: transacciones, estados y calendario


### Ejercicio 2
1. Los archivos que preparan los datos para consulta dentro de snowflake están en la carpeta exercise_2.
Se procesan los jsons que se reciben de s3 para darles formato de tabla dentro de snowflake

### Ejercicio 3
1. Las vistas las realicé directamente en tableau y dentro de tableau diseñé el dashboard

