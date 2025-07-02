from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

import sys

if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(-1)

    input_folder = sys.argv[1]
    output_folder = sys.argv[2]

    spark = SparkSession.builder.appName("CategoriaDeVideosMasVista").getOrCreate()

    # Lee el CSV
    df = spark.read.csv(input_folder, header=True, inferSchema=True)

    # Agrupa por categoría y suma las vistas
    categoria_vistas = df.groupBy("category").agg(spark_sum("views").alias("total_views"))

    # Ordena y toma la más vista
    categoria_mas_vista = categoria_vistas.orderBy(col("total_views").desc()).limit(1)

    # Guarda el resultado
    categoria_mas_vista.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_folder)

    spark.stop()