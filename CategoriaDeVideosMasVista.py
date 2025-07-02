from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

import sys

if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(-1)

    input_folder = sys.argv[1]
    output_folder = sys.argv[2]

    spark = SparkSession.builder.appName("CategoriaDeVideosMasVista").getOrCreate()

    # Nombres de columnas en orden (porque el archivo no tiene encabezado)
    columnas = [
        "video_id", "uploader", "age", "category", "length",
        "views", "rate", "ratings", "comments", "related_ids"
    ]

    # Leer CSV sin encabezado
    df = spark.read.csv(f"{input_folder}/*.txt", header=False, inferSchema=True, sep="\t")

    # Asignar nombres de columnas
    df = df.toDF(*columnas)

    # Agrupar por categoría y sumar las vistas
    categoria_vistas = df.groupBy("category").agg(spark_sum("views").alias("total_views"))

    # Ordenar y tomar la categoría con más vistas
    categoria_mas_vista = categoria_vistas.orderBy(col("total_views").desc()).limit(1)

    # Guardar resultado
    categoria_mas_vista.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_folder)

    spark.stop()