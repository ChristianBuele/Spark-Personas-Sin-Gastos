from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.functions import lower, trim

hdfs_path = "hdfs://localhost:9000/ejercicio3"

def menores_1500(df):# b) Compras con TDC y <= 1500 euros
    menor_igual_1500 = df.filter(col("dinero_gastado") <= 1500)
    return menor_igual_1500.groupBy("persona").agg(count("*").alias("compras_menor_igual_1500"))

def mayores_1500(df):# a) Compras con TDC y > 1500 euros
    mayor_1500 = df.filter(col("dinero_gastado") > 1500)
    return mayor_1500.groupBy("persona").agg(count("*").alias("compras_mayor_1500"))

def read_input(spark):
    df = spark.read.option("delimiter", ";").option("header", "false").csv(hdfs_path + "/data/casoDePrueba3.txt")
    df = df.toDF("persona", "metodo_pago", "dinero_gastado")
    df.show()

    df = df.withColumn("dinero_gastado", col("dinero_gastado").cast("float"))  # Convertir dinero_gastado a float
    return df

def main():
    spark = SparkSession.builder.appName("PersonaYMetodosDePago").getOrCreate()
    df = read_input(spark)
    
    df_tdc = df.filter(lower(trim(col("metodo_pago"))) == "tarjeta de crédito")# Filtrar solo compras con tarjeta de crédito
    
    res_menor = menores_1500(df_tdc)
    res_menor.write.mode("overwrite").csv(hdfs_path + "/output/comprasConTDCMenoroIgualDe1500")
    
    res_mayor = mayores_1500(df_tdc)
    res_mayor.write.mode("overwrite").csv(hdfs_path + "/output/comprasConTDCMayorDe1500")
    
    spark.stop()

if __name__ == "__main__":
    main()