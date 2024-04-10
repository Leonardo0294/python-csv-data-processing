import csv

# Se abre el archivo CSV en modo lectura
with open('localidades.csv', newline='') as archivo_csv:
    # Lector CSV especificando el delimitador y el carácter de cita
    lector_csv = csv.reader(archivo_csv, delimiter=',', quotechar='"')
    
    # Iterar sobre cada fila en el archivo CSV
    for fila in lector_csv:
        # Se imprime la fila en la consola
        print(fila)
