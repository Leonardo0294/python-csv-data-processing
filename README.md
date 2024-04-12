# Script de Python para Interacción con Base de Datos MariaDB

Este script de Python permite realizar operaciones básicas de interacción con una base de datos MariaDB, incluyendo la lectura de datos desde un archivo CSV, la creación de una tabla en la base de datos, la inserción de datos en la tabla, y la exportación de datos a archivos CSV separados por provincia.

## Requisitos

- Python 3.x instalado en tu sistema.
- MariaDB Server instalado y configurado localmente.

## Instalación

1. **Clonar el Repositorio:**

   Clona este repositorio

   ```bash
   git clone https://github.com/Leonardo0294/python-csv-data-processing.git


Instalar Dependencias:

Instala las dependencias necesarias utilizando pip:

 ```bash
pip install mariadb

Uso
Preparación:
        Coloca tu archivo CSV de datos (localidades.csv) en el mismo directorio que el script main.py.
        Asegúrate de tener el servidor MariaDB en ejecución localmente.

    Ejecutar el Script:

    Ejecuta el script main.py desde la línea de comandos:

    ```bash

    python main.py

Funcionalidades

    leer_csv(nombre_archivo): Lee los datos desde un archivo CSV y devuelve una lista de diccionarios.

    conectar_db(): Establece una conexión a la base de datos MariaDB.

    crear_tabla(conn): Crea la tabla Localidades en la base de datos MariaDB.

    insertar_datos(conn, data): Inserta los datos del archivo CSV en la tabla Localidades.

    exportar_csv_por_provincia(conn): Exporta datos a archivos CSV separados por provincia desde la tabla Localidades.