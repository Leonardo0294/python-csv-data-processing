import csv
import mariadb
import os

def leer_datos_desde_csv(nombre_archivo):
    """Lee los datos desde un archivo CSV y devuelve una lista de diccionarios."""
    datos = []
    with open(nombre_archivo, newline='', encoding='utf-8') as archivo_csv:
        lector_csv = csv.DictReader(archivo_csv)
        for fila in lector_csv:
            datos.append(fila)
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
    """Inserta datos en la tabla 'Localidades'."""
    try:
        cursor = conexion.cursor()
        for fila in datos:
            provincia = fila['provincia']
            localidad = fila['localidad']
            cursor.execute("INSERT INTO Localidades (Provincia, Localidad) VALUES (?, ?)", (provincia, localidad))
        conexion.commit()
        print('Datos insertados correctamente en la tabla "Localidades".')
    except mariadb.Error as error:
        print(f"Error al insertar datos: {error}")

def exportar_csv_por_provincia(conexion):
    """Exporta datos a archivos CSV por provincia."""
    try:
        cursor = conexion.cursor()
        cursor.execute("SELECT DISTINCT Provincia FROM Localidades")
        provincias = cursor.fetchall()

        if not os.path.exists('csv_exports'):
            os.makedirs('csv_exports')

        for provincia in provincias:
            provincia_nombre = provincia[0]
            cursor.execute("SELECT Localidad FROM Localidades WHERE Provincia = ?", (provincia_nombre,))
            localidades = cursor.fetchall()

            with open(f'csv_exports/{provincia_nombre}.csv', 'w', newline='', encoding='utf-8') as archivo_csv:
                escritor_csv = csv.writer(archivo_csv)
                escritor_csv.writerow(['Localidad'])
                escritor_csv.writerows(localidades)

            print(f"Archivo CSV exportado para la provincia: {provincia_nombre}")

    except mariadb.Error as error:
        print(f"Error al exportar archivos CSV por provincia: {error}")

def main():
    # Nombre del archivo CSV
    nombre_archivo = 'localidades.csv'

    # Leer datos desde el archivo CSV
    datos_csv = leer_datos_desde_csv(nombre_archivo)

    # Conectar a la base de datos
    conexion = conectar_base_de_datos(
        host='localhost',
        puerto=3306,
        usuario='root',
        contraseña='',
        nombre_base_datos='db_python'
    )
    if not conexion:
        return

    # Crear la tabla 'Localidades'
    crear_tabla_localidades(conexion)

    # Insertar datos en la tabla 'Localidades'
    insertar_datos_en_tabla(conexion, datos_csv)

    # Exportar datos a archivos CSV por provincia
    exportar_csv_por_provincia(conexion)

    # Cerrar la conexión
    conexion.close()
    print('Proceso completado con éxito.')

if __name__ == '__main__':
    main()
