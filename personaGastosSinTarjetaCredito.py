from pyspark import SparkContext

def splitData(line:str):
    """
    Split the input line into a list of values.
    """
    objects = line.strip().split(";")
    if len(objects) == 3:
        person, payment_method, amount = objects
    else:
        return None
    
    try:
        amount = float(amount)
    except ValueError:
        return None
    return (person, payment_method, amount)

def filter(line):
    """
    Filter out lines that do not contain 'tarjeta de credito'.
    """
    data=splitData(line)
    if data is None:
        return []
    person, payment_method, amount = data
    if payment_method.lower() != "tarjeta de crédito":
        return [(person, amount)]
    return [(person, 0.0)]

def main():
    """
    Main function to process the input data and calculate total expenses per person.
    """
    sc = SparkContext("local", "Persona Gastos Sin Tarjeta de Credito")
    
    # Read the input file
    rdd = sc.textFile("casoDePrueba1.txt")
    
   # Filtrado y mapeo eficiente
    gastos = (
        rdd.flatMap(filter)  # mapea línea a (persona, dinero) o (persona, 0.0)
           .reduceByKey(lambda a, b: a + b)  # suma por persona
           .map(lambda x: f"{x[0]};{int(x[1])}")  # convierte a string con formato deseado
    )

    gastos.saveAsTextFile("ruta/a/salida.txt")  # guarda la salida en HDFS o local
    sc.stop()
   
if __name__ == "__main__":
    main()