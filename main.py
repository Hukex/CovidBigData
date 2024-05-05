import os
import sys
import random
import numpy as np
import warnings
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
from pyspark.sql.types import (
    NumericType,
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    FloatType
)
from datetime import datetime, timedelta
from typing import List
import requests


FECHA_COVID = '20200624'
N_DIAS = 128

warnings.filterwarnings("ignore")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

def main():
    url_diaria_covid = f"{URL}/us/{FECHA_COVID}.json"
    respuesta_diaria_covid = realizar_solicitud_get(url=url_diaria_covid)
    guardar_en_archivo(respuesta_diaria_covid.text,
                       "covid_diario", "json")

    spark = SparkSession.builder \
        .appName("jsoncovid") \
        .getOrCreate()

    generar_csv_ciudades_paises(spark=spark)

    ruta_covid_diario = os.path.join(os.getcwd(), "covid_diario.json")
    df = spark.read.option("multiline", True).json(ruta_covid_diario)
    df.createOrReplaceTempView("json")

    columnas_numericas = [
        col.name for col in df.schema.fields if isinstance(col.dataType, NumericType)
    ]

    esquema = StructType(
        [
            *[
                StructField(nombre_col, df.schema[nombre_col].dataType, True)
                for nombre_col in [*columnas_numericas, "dateChecked"]
            ],
            StructField("id_ciudad", IntegerType(), True),
            StructField("factor_multiplicador", FloatType(), True),
            StructField("año", StringType(), True),
            StructField("mes", StringType(), True),
            StructField("día", StringType(), True),
            StructField("f_procesado", BooleanType(), True)
        ]
    )

    try:
        filas_a_agregar = []

        for i in range(6):
            ciudad_id = i
            filas = df.select(*columnas_numericas, "dateChecked").collect()[0]
            fila = filas.asDict()

            fechas = generar_fechas_n(fecha=fila["date"], n_veces=N_DIAS)
            dict_fila = {}

            for j in range(N_DIAS):
                factor_m = random.uniform(1, 1.8)
                for clave in fila:
                    fecha = str(fechas[j])
                    año = int(fecha[0:4])
                    mes = int(fecha[4:6])
                    día = int(fecha[6:])

                    if clave == "date":
                        dict_fila[clave] = fechas[j]
                    elif clave == "dateChecked":
                        dict_fila[clave] = fila[clave]
                    else:
                        nuevo_valor = int(np.floor(fila[clave] * factor_m))
                        dict_fila[clave] = nuevo_valor

                dict_fila["id_ciudad"] = ciudad_id+1
                dict_fila["factor_multiplicador"] = factor_m
                dict_fila["año"] = año
                dict_fila["mes"] = mes
                dict_fila["día"] = día
                dict_fila["f_procesado"] = True

                filas_a_agregar.append(Row(**dict_fila))

        df_parquet = spark.createDataFrame(filas_a_agregar, esquema)
        df_parquet.createOrReplaceTempView("parquet")

        df_parquet.toPandas().to_parquet("info_covid.parquet")
        print("Parquet guardado, nombre: info_covid.parquet :)")
    except Exception as e:
        print("Error al intentar guardar datos en archivo parquet: {}".format(e))


def generar_fechas_n(fecha: int, n_veces: int = 120) -> List[int]:
    try:
        fecha_str = str(fecha)
        año = int(fecha_str[:4])
        mes = int(fecha_str[4:6])
        dia = int(fecha_str[6:])
        fechas = []

        for i in range(n_veces):
            fecha_actual = datetime(año, mes, dia) + timedelta(days=i)
            año_nuevo = fecha_actual.year
            mes_nuevo = fecha_actual.month
            dia_nuevo = fecha_actual.day
            nueva_fecha = int(f"{año_nuevo}{mes_nuevo:02d}{dia_nuevo:02d}")
            fechas.append(nueva_fecha)

        return fechas
    except Exception as e:
        print(f"Error al generar fechas: {e}")


def generar_csv_ciudades_paises(spark):
    ciudades = [
        # España
        Row(
            id=1,
            id_pais=1,
            nombre="Madrid",
            latitud="40.4168",
            longitud="-3.7038",
        ),
        Row(
            id=2,
            id_pais=1,
            nombre="Barcelona",
            latitud="41.3851",
            longitud="2.1734",
        ),
        Row(
            id=3,
            id_pais=1,
            nombre="Valencia",
            latitud="39.4699",
            longitud="-0.3763",
        ),
        Row(
            id=4,
            id_pais=1,
            nombre="Sevilla",
            latitud="37.3886",
            longitud="-5.9822",
        ),
        Row(
            id=5,
            id_pais=1,
            nombre="Bilbao",
            latitud="43.2630",
            longitud="-2.9350",
        ),
        Row(
            id=6,
            id_pais=1,
            nombre="Málaga",
            latitud="36.7213",
            longitud="-4.4215",
        ),
        # Francia
        Row(
            id=7,
            id_pais=2,
            nombre="París",
            latitud="48.8566",
            longitud="2.3522",
        ),
        Row(
            id=8,
            id_pais=2,
            nombre="Marsella",
            latitud="43.2965",
            longitud="5.3698",
        ),
        Row(
            id=9,
            id_pais=2,
            nombre="Lyon",
            latitud="45.7578",
            longitud="4.8320",
        ),
        Row(
            id=10,
            id_pais=2,
            nombre="Toulouse",
            latitud="43.6047",
            longitud="1.4442",
        ),
        Row(
            id=11,
            id_pais=2,
            nombre="Niza",
            latitud="43.7102",
            longitud="7.2620",
        ),
        Row(
            id=12,
            id_pais=2,
            nombre="Nantes",
            latitud="47.2184",
            longitud="-1.5536",
        ),
        # Estados Unidos
        Row(
            id=13,
            id_pais=3,
            nombre="Nueva York",
            latitud="40.7128",
            longitud="-74.0060",
        ),
        Row(
            id=14,
            id_pais=3,
            nombre="Los Ángeles",
            latitud="34.0522",
            longitud="-118.2437",
        ),
        Row(
            id=15,
            id_pais=3,
            nombre="Chicago",
            latitud="41.8781",
            longitud="-87.6298",
        ),
        Row(
            id=16,
            id_pais=3,
            nombre="Houston",
            latitud="29.7604",
            longitud="-95.3698",
        ),
        Row(
            id=17,
            id_pais=3,
            nombre="Miami",
            latitud="25.7617",
            longitud="-80.1918",
        ),
        Row(
            id=18,
            id_pais=3,
            nombre="Las Vegas",
            latitud="36.1699",
            longitud="-115.1398",
        ),
        # Japón
        Row(
            id=19,
            id_pais=4,
            nombre="Tokio",
            latitud="35.6895",
            longitud="139.6917",
        ),
        Row(
            id=20,
            id_pais=4,
            nombre="Kioto",
            latitud="35.0116",
            longitud="135.7681",
        ),
        Row(
            id=21,
            id_pais=4,
            nombre="Osaka",
            latitud="34.6937",
            longitud="135.5023",
        ),
        Row(
            id=22,
            id_pais=4,
            nombre="Sapporo",
            latitud="43.0618",
            longitud="141.3545",
        ),
        Row(
            id=23,
            id_pais=4,
            nombre="Nagoya",
            latitud="35.1815",
            longitud="136.9066",
        ),
        Row(
            id=24,
            id_pais=4,
            nombre="Fukuoka",
            latitud="33.5904",
            longitud="130.4017",
        ),
        # Reino Unido
        Row(
            id=25,
            id_pais=5,
            nombre="Londres",
            latitud="51.5074",
            longitud="-0.1278",
        ),
        Row(
            id=26,
            id_pais=5,
            nombre="Birmingham",
            latitud="52.4862",
            longitud="-1.8904",
        ),
        Row(
            id=27,
            id_pais=5,
            nombre="Manchester",
            latitud="53.4808",
            longitud="-2.2426",
        ),
        Row(
            id=28,
            id_pais=5,
            nombre="Liverpool",
            latitud="53.4084",
            longitud="-2.9916",
        ),
        Row(
            id=29,
            id_pais=5,
            nombre="Glasgow",
            latitud="55.8642",
            longitud="-4.2518",
        ),
        Row(
            id=30,
            id_pais=5,
            nombre="Edimburgo",
            latitud="55.9533",
            longitud="-3.1883",
        ),
        # Australia
        Row(
            id=31,
            id_pais=6,
            nombre="Sídney",
            latitud="-33.8688",
            longitud="151.2093",
        ),
        Row(
            id=32,
            id_pais=6,
            nombre="Melbourne",
            latitud="-37.8136",
            longitud="144.9631",
        ),
        Row(
            id=33,
            id_pais=6,
            nombre="Brisbane",
            latitud="-27.4698",
            longitud="153.0251",
        ),
        Row(
            id=34,
            id_pais=6,
            nombre="Perth",
            latitud="-31.9505",
            longitud="115.8605",
        ),
        Row(
            id=35,
            id_pais=6,
            nombre="Adelaida",
            latitud="-34.9285",
            longitud="138.6007",
        ),
        Row(
            id=36,
            id_pais=6,
            nombre="Canberra",
            latitud="-35.2809",
            longitud="149.1300",
        ),
    ]

    paises = [
        Row(
            id=1,
            nombre="España",
        ),
        Row(
            id=2,
            nombre="Francia",
        ),
        Row(
            id=3,
            nombre="Estados Unidos",
        ),
        Row(id=4, nombre="Japón"),
        Row(id=5, nombre="Reino Unido"),
        Row(
            id=6,
            nombre="Australia",
        ),
    ]

    df_ciudades = spark.createDataFrame(
        ciudades,
        StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("id_pais", IntegerType(), True),
                StructField("nombre", StringType(), True),
                StructField("latitud", StringType(), True),
                StructField("longitud", StringType(), True),
            ]
        ),
    )

    df_paises = spark.createDataFrame(
        paises,
        StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("nombre", StringType(), True),
            ]
        ),
    )

    df_ciudades.toPandas().to_csv("ciudades.csv", index=False)
    df_paises.toPandas().to_csv("paises.csv", index=False)


URL = "https://api.covidtracking.com/v1"


def realizar_solicitud_get(url: str, params=None):
    res = requests.get(url, params=params)
    return res


def guardar_en_archivo(
    texto=None, nombre_salida="salida", extension="json"
):
    try:
        if extension.startswith("."):
            extension = extension[1:]

        with open(f"{nombre_salida}.{extension}", "w") as archivo:
            if texto is None:
                raise BaseException(
                    "El texto no puede ser de tipo None, debe ser str.")
            else:
                archivo.write(texto)
    except Exception as e:
        print(f"Error al intentar crear un archivo: {e}")


if __name__ == "__main__":
    main()
