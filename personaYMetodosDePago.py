from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

hdfs_path = "hdfs://localhost:9000/ejercicio3"

# Inicializar Spark
spark = SparkSession.builder.appName("PersonaYMetodosDePago").getOrCreate()

# Leer los datos (sin cabecera, usando el delimitador correcto)
df = spark.read.option("delimiter", ";").csv(hdfs_path + "/data/casoDePrueba3.txt")

# Asignar nombres a las columnas
df = df.toDF("persona", "metodo_pago", "dinero_gastado")

# Convertir dinero_gastado a float
df = df.withColumn("dinero_gastado", col("dinero_gastado").cast("float"))

# Filtrar solo compras con tarjeta de crédito
df_tdc = df.filter(col("metodo_pago") == "tarjeta_credito")

# a) Compras con TDC y > 1500 euros
mayor_1500 = df_tdc.filter(col("dinero_gastado") > 1500)
res_mayor = mayor_1500.groupBy("persona").agg(count("*").alias("compras_mayor_1500"))

# Guardar resultado en HDFS
res_mayor.write.mode("overwrite").csv(hdfs_path + "/comprasConTDCMayorDe1500")

# b) Compras con TDC y <= 1500 euros
menor_igual_1500 = df_tdc.filter(col("dinero_gastado") <= 1500)
res_menor = menor_igual_1500.groupBy("persona").agg(count("*").alias("compras_menor_igual_1500"))

# Guardar resultado en HDFS
res_menor.write.mode("overwrite").csv(hdfs_path + "/comprasConTDCMenoroIgualDe1500")

spark.stop()