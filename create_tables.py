import psycopg2
from config import config

# Cargamos la configuracion para conectar con la base de datos postgres
conn = psycopg2.connect(**config)
cur = conn.cursor()

# Confirmamos conexion imprimendo la version
cur.execute('SELECT version()')
version = cur.fetchone()
print(version)

# Ejecutamos el script sql para crear las tres tablas.
file = open('create_tables.sql').read()
rows = file.split(';')[:-1]  # Último elemento siempre esta vacio

# Ejecutamos los comandos en script.sql
for row in rows:
    cur.execute(row)
    conn.commit()
