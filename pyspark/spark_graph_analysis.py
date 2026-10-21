from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = (SparkSession.builder
             .appName("Modulo_3_GraphAnalysis")
             .master("local[*]")
             .getOrCreate())
    
    spark.sparkContext.setLogLevel("WARN")
    print("=========================================================")
    print("=== PILAR 3: ANÁLISE DE REDES E GRAFOS (CONCEITO GRAPHX) ==")
    print("=========================================================")

    sales_data = [
        ("GOOG", "Sam", 200.0),
        ("GOOG", "Charlie", 120.0),
        ("GOOG", "Frank", 340.0),
        ("MSFT", "Tina", 600.0),
        ("MSFT", "Amy", 124.0),
        ("MSFT", "Vanessa", 243.0),
        ("FB", "Carl", 870.0),
        ("FB", "Sarah", 350.0),
        ("APPL", "John", 250.0),
        ("APPL", "Linda", 130.0),
        ("APPL", "Mike", 750.0),
        ("APPL", "Chris", 350.0)
    ]
    df = spark.createDataFrame(sales_data, ["Company", "Person", "Sales"])

    # Definição de Vértices (Pessoas)
    vertices = df.select(col("Person").alias("id")).distinct()
    print("\n--- Vértices (Entidades Únicas) ---")
    vertices.show(5)

    # Definição de Arestas (Relações de Transação entre Origem e Destino)
    edges = df.select(
        col("Person").alias("src"), 
        col("Company").alias("dst"), 
        col("Sales").alias("weight")
    )

    print("--- Arestas Filtradas (Conexões de Transações de Alto Valor > 300) ---")
    edges.filter(col("weight") > 300).show()

    spark.stop()

if __name__ == "__main__":
    main()