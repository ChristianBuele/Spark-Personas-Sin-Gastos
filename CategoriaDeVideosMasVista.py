from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum
import sys

HDFS_BASE_PATH = "hdfs://localhost:9000/ejercicio2/"

if __name__ == "__main__":
    if len(sys.argv) != 3: #valido si envia los argumentos necesarios
        sys.exit(-1)

    input_path = sys.argv[1] # Ruta del archivo de entrada
    output_path = sys.argv[2] # Ruta del archivo de salida

    spark = SparkSession.builder.appName("CategoriaDeVideosMasVista").getOrCreate()

    # Leer archivo TSV sin cabecera
    df = spark.read.option("delimiter", "\t").csv(HDFS_BASE_PATH+input_path)

    # Renombrar columnas necesarias
    df = df.withColumnRenamed("_c3", "categoria").withColumnRenamed("_c5", "vistas")

    # Cast a tipo entero la columna de vistas
    df = df.withColumn("vistas", col("vistas").cast("int"))

    # Agrupar por categoría y sumar vistas
    categoria_vistas = df.groupBy("categoria").agg(_sum("vistas").alias("total_vistas"))

    # Obtener la categoría más vista
    resultado = categoria_vistas.orderBy(col("total_vistas").desc()).limit(1)

    # Escribir el resultado en formato CSV 
    resultado.write.option("header", True).csv(HDFS_BASE_PATH+output_path+"/")

    spark.stop()
