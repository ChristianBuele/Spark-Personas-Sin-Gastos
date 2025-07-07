from pyspark import SparkContext

PATH_HDFS = "hdfs://localhost:9000/ejercicio1/"

def parse_and_filter(line: str):
    """
    Parsea una línea y retorna un (persona, cantidad) si el método de pago no es 'tarjeta de crédito'.
    Retorna None si la línea está mal formada o si el método de pago es 'tarjeta de crédito'.
    """
    parts = line.strip().split(";")
    if len(parts) != 3:
        return None

    person, payment_method, amount_str = parts
    try:
        amount = float(amount_str)
    except ValueError:
        return None

    if payment_method.lower().strip() != "tarjeta de crédito":
        return (person, amount)
    return None  # No se emite si es tarjeta de crédito

def main():
    sc = SparkContext("local", "Gastos Sin Tarjeta de Credito")

    # Leer y procesar datos
    gastos = (
        sc.textFile(PATH_HDFS + "data/casoDePrueba1.txt")
          .map(parse_and_filter)
          .filter(lambda x: x is not None)  # Filtrar nulos
          .reduceByKey(lambda a, b: a + b)  # Sumar cantidades por persona
          .map(lambda x: f"{x[0]};{int(x[1])}")  # Formato final
    )

    # Guardar salida
    gastos.saveAsTextFile(PATH_HDFS + "output/personas")
    sc.stop()

if __name__ == "__main__":
    main()