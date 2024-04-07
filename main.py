import csv
import mysql.connector
from mysql.connector import Error
import os

# Función para leer el archivo CSV
def leer_csv(nombre_archivo):
    data = []
    with open(nombre_archivo, newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(row)
    return data

# Función para crear la conexión a la base de datos
def conectar_db():
    try:
        conn = mysql.connector.connect(
            host='localhost',
            port=3306,  
            user='root',
            password='',
            database='db_python'  
        )
        if conn.is_connected():
            print('Conexión establecida con la base de datos.')
            return conn
    except Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

# Función para crear la tabla en la base de datos
def crear_tabla(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS Localidades")
        cursor.execute("""
            CREATE TABLE Localidades (
                Provincia VARCHAR(255),
                Localidad VARCHAR(255)
            )
        """)
        conn.commit()
        print('Tabla creada correctamente.')
    except Error as e:
        print(f"Error al crear la tabla: {e}")

# Función para insertar datos en la tabla
def insertar_datos(conn, data):
    try:
        cursor = conn.cursor()
        for row in data:
            provincia = row['provincia']
            localidad = row['localidad']
            cursor.execute("INSERT INTO Localidades (Provincia, Localidad) VALUES (%s, %s)", (provincia, localidad))
        conn.commit()
        print('Datos insertados correctamente.')
    except Error as e:
        print(f"Error al insertar datos: {e}")

# Función para exportar datos a archivos CSV por provincia
def exportar_csv_por_provincia(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT Provincia FROM Localidades")
        provincias = cursor.fetchall()

        if not os.path.exists('csv_exports'):
            os.makedirs('csv_exports')

        for provincia in provincias:
            provincia_nombre = provincia[0]
            cursor.execute("SELECT Localidad FROM Localidades WHERE Provincia = %s", (provincia_nombre,))
            localidades = cursor.fetchall()

            with open(f'csv_exports/{provincia_nombre}.csv', 'w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(['Localidad'])
                writer.writerows(localidades)

            print(f"Archivo CSV exportado para la provincia: {provincia_nombre}")

    except Error as e:
        print(f"Error al exportar archivos CSV por provincia: {e}")

# Función principal
def main():
    nombre_archivo = 'localidades.csv'

    # Leer el archivo CSV
    data = leer_csv(nombre_archivo)

    # Conectar a la base de datos
    conn = conectar_db()
    if not conn:
        return

    # Crear la tabla en la base de datos
    crear_tabla(conn)

    # Insertar datos en la tabla
    insertar_datos(conn, data)

    # Exportar datos a archivos CSV por provincia
    exportar_csv_por_provincia(conn)

    # Cerrar la conexión
    conn.close()
    print('Proceso completado con éxito.')

if __name__ == '__main__':
    main()
