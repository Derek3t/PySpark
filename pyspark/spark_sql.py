from pyspark.sql import SparkSession

def main():
    # Inicialização da SparkSession
    spark = (SparkSession.builder
             .appName("Modulo_1_SparkSQL")
             .master("local[*]")
             .getOrCreate())
    
    spark.sparkContext.setLogLevel("WARN")
    print("=========================================================")
    print("=== PILAR 1: SPARK SQL E PROCESSAMENTO DE DATAFRAMES  ===")
    print("=========================================================")

    # Dataset Inicial de Vendas (Baseado em sales_info.csv)
    sales_data = [
        ("GOOG", "Sam", 200.0, "2024-01-10"),
        ("GOOG", "Charlie", 120.0, "2024-01-15"),
        ("GOOG", "Frank", 340.0, "2025-02-20"),
        ("MSFT", "Tina", 600.0, "2024-03-05"),
        ("MSFT", "Amy", 124.0, "2025-04-10"),
        ("MSFT", "Vanessa", 243.0, "2025-05-12"),
        ("FB", "Carl", 870.0, "2024-06-15"),
        ("FB", "Sarah", 350.0, "2025-07-18"),
        ("APPL", "John", 250.0, "2024-08-22"),
        ("APPL", "Linda", 130.0, "2025-09-25"),
        ("APPL", "Mike", 750.0, "2024-10-30"),
        ("APPL", "Chris", 350.0, "2025-11-05")
    ]
    columns = ["Company", "Person", "Sales", "Date"]
    df = spark.createDataFrame(sales_data, columns)

    # Registro de Tabela Temporária na Memória
    df.createOrReplaceTempView("vendas")

    # Consulta ANSI SQL com Filtro, Agregação e Ordenação
    query_sql = """
        SELECT Company, AVG(Sales) as Media_Vendas, COUNT(1) as Total_Transacoes
        FROM vendas
        GROUP BY Company
        HAVING Media_Vendas > 200
        ORDER BY Media_Vendas DESC
    """
    
    print("\n--- Resultado da Consulta SQL em Memória ---")
    spark.sql(query_sql).show()

    spark.stop()

if __name__ == "__main__":
    main()