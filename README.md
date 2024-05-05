# Práctica COVID

En esta práctica implementaremos un proyecto real relacionado con el seguimiento de datos de COVID-19. Para ello, se requiere tener un repositorio de Git donde se almacenen todo el código y los archivos de Power BI sin datos. Esto significa que solo se subirá el modelo, y para cargar datos, se deberá tener un parámetro que permita cargar los datos.

## Objetivo
El objetivo principal es desarrollar un sistema que obtenga datos de una API, los procese utilizando PySpark y Pentaho, y los visualice en un cuadro de mando informativo utilizando Power BI.

## Pasos a seguir:

1. **Creación de Repositorio GIT**: Crea un repositorio Git donde almacenaremos todo el código y los archivos necesarios para este proyecto.

2. **Obtención de Datos de la API**: Utilizaremos Python para acceder a la API.COVIDTRACKING.COM y obtener datos específicos para una fecha determinada. Es importante comprender la estructura de la API y cómo acceder a los datos requeridos.

3. **Transformación de Datos**: Los datos obtenidos de la API se guardarán en formato JSON. Utilizaremos Spark para leer este JSON, agregar información sobre país, ciudad y un factor multiplicador, y guardar los datos resultantes en formato Parquet. Se deben generar dos archivos independientes: uno para la tabla de ciudad y otro para la tabla de país. Es importante que las relaciones entre las ciudades y los países sean coherentes.

4. **Procesamiento de Datos Históricos**: Utilizaremos Pentaho o un script Spark para leer los archivos Parquet generados anteriormente. Se agregarán columnas de fecha de procesamiento (F_procesado), así como columnas para el año, mes y día de los datos. Los datos procesados se guardarán en archivos Parquet históricos, con la posibilidad de tener un archivo para cada país-ciudad. Se debe tener datos para un período de 4 meses.

5. **Configuración de Power BI**: Power BI se utilizará para visualizar los datos históricos. Se leerán los archivos Parquet históricos, así como los archivos de ciudad y país. Se montará un modelo de datos y se aplicará un RLS (Row-Level Security) con 4 roles diferentes: un rol que pueda ver datos de 2 países, otro que pueda ver todos los datos y dos roles adicionales que solo puedan ver datos de un país cada uno. Es fundamental que el modelo de datos se guarde sin datos y que tenga un parámetro en la carga de datos para determinar si se deben cargar datos o no.

6. **Generación de Cuadro de Mando Informativo**: Se creará un cuadro de mando informativo en Power BI con los datos disponibles, mostrando la información relevante de manera clara y concisa.

## Consejos Adicionales:

- Es recomendable utilizar una base de datos para cargar el histórico de datos.
- Todos los archivos procesados deben moverse a una carpeta designada (por ejemplo, "old" o "procesado") para mantener un registro claro de los archivos que han sido procesados.

¡Manos a la obra y buena suerte con el proyecto! Si necesitas más detalles sobre algún paso en particular, no dudes en preguntar.
