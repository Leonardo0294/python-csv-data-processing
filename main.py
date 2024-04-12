import csv
import mariadb
import os
import time

def leer_datos_desde_csv(nombre_archivo):
    """Lee los datos desde un archivo CSV y devuelve una lista de diccionarios."""
    datos = []
    with open(nombre_archivo, newline='', encoding='utf-8') as archivo_csv:
        lector_csv = csv.DictReader(archivo_csv)
        datos = list(lector_csv)  
    return datos

def conectar_base_de_datos(host, puerto, usuario, contraseña, nombre_base_datos):
    """Establece una conexión a la base de datos MariaDB."""
    try:
        conexion = mariadb.connect(
            host=host,
            port=puerto,
            user=usuario,
            password=contraseña,
            database=nombre_base_datos
        )
        print('Conexión establecida con la base de datos.')
        return conexion
    except mariadb.Error as error:
        print(f"Error al conectar a la base de datos: {error}")
        return None

def crear_tabla_localidades(conexion):
    """Crea la tabla 'Localidades' en la base de datos."""
    try:
        cursor = conexion.cursor()
        cursor.execute("DROP TABLE IF EXISTS Localidades")
        cursor.execute("""
            CREATE TABLE Localidades (
                Provincia VARCHAR(255),
                Localidad VARCHAR(255)
            )
        """)
        conexion.commit()
        print('Se eliminó la tabla existente y se creó una nueva tabla "Localidades".')
    except mariadb.Error as error:
        print(f"Error al crear la tabla: {error}")

def insertar_datos_en_tabla(conexion, datos):
    """Inserta datos en la tabla 'Localidades' utilizando una transacción."""
    try:
        cursor = conexion.cursor()
        cursor.execute("START TRANSACTION")

        valores = [(fila['provincia'], fila['localidad']) for fila in datos]
        cursor.executemany("INSERT INTO Localidades (Provincia, Localidad) VALUES (?, ?)", valores)

        conexion.commit()
        print('Datos insertados correctamente en la tabla "Localidades".')
    except mariadb.Error as error:
        conexion.rollback()
        print(f"Error al insertar datos: {error}")

def exportar_csv_por_provincia(conexion):
    """Exporta datos a archivos CSV por provincia y cuenta los archivos exportados."""
    try:
        cursor = conexion.cursor()
        cursor.execute("SELECT DISTINCT Provincia FROM Localidades")
        provincias = [provincia[0] for provincia in cursor.fetchall()]

        if not os.path.exists('csv_exports'):
            os.makedirs('csv_exports')

        archivos_exportados = 0

        # Se utiliza executemany para obtener localidades por provincia
        query_localidades = "SELECT Localidad FROM Localidades WHERE Provincia = ?"
        for provincia in provincias:
            cursor.execute(query_localidades, (provincia,))
            localidades = [localidad[0] for localidad in cursor.fetchall()]

            with open(f'csv_exports/{provincia}.csv', 'w', newline='', encoding='utf-8') as archivo_csv:
                escritor_csv = csv.writer(archivo_csv)
                escritor_csv.writerow(['Localidad'])
                escritor_csv.writerows(zip(localidades))

            archivos_exportados += 1

        if archivos_exportados > 0:
            print(f"{archivos_exportados} archivo(s) CSV exportado(s) con éxito.")
        else:
            print("No se exportaron archivos CSV.")

    except mariadb.Error as error:
        print(f"Error al exportar archivos CSV por provincia: {error}")

def main():
    # Nombre del archivo CSV
    nombre_archivo = 'localidades.csv'

    # Se leen datos desde el archivo CSV
    datos_csv = leer_datos_desde_csv(nombre_archivo)

    # Conexion a la base de datos
    conexion = conectar_base_de_datos(
        host='localhost',
        puerto=3306,
        usuario='root',
        contraseña='',
        nombre_base_datos='db_python'
    )
    if not conexion:
        return

    try:
        # Se crea la tabla 'Localidades'
        crear_tabla_localidades(conexion)

        # Se insertan datos en la tabla Localidades
        start_time = time.time()
        insertar_datos_en_tabla(conexion, datos_csv)
        print(f"Tiempo de inserción en la base de datos: {time.time() - start_time:.2f} segundos")

        # Se exportan los datos a archivos CSV por provincia
        start_time = time.time()
        exportar_csv_por_provincia(conexion)
        print(f"Tiempo de exportación de archivos CSV: {time.time() - start_time:.2f} segundos")

    finally:
        # Cierre de la conexión
        conexion.close()
        print('Proceso completado con éxito.')

if __name__ == '__main__':
    main()
