from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum
import sys

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: CategoriaDeVideosMasVista <input_path> <output_path>", file=sys.stderr)
        sys.exit(-1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder.appName("CategoriaDeVideosMasVista").getOrCreate()

    # Cargar el archivo sin cabecera (tabulado)
    df = spark.read.option("delimiter", "\t").csv(input_path)

    # Asignar nombres a las columnas necesarias (col 3: categoría, col 5: vistas)
    df = df.withColumnRenamed("_c3", "categoria").withColumnRenamed("_c5", "vistas")

    # Convertir la columna de vistas a entero
    df = df.withColumn("vistas", col("vistas").cast("int"))

    # Agrupar por categoría y sumar las vistas
    categoria_vistas = df.groupBy("categoria").agg(_sum("vistas").alias("total_vistas"))

    # Obtener la categoría más vista
    resultado = categoria_vistas.orderBy(col("total_vistas").desc()).limit(1)

    # Guardar el resultado en formato CSV
    resultado.coalesce(1).write.option("header", True).csv(output_path)

    spark.stop()