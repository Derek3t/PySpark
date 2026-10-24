from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

def main():
    spark = (SparkSession.builder
             .appName("Modulo_2_MLlib")
             .master("local[*]")
             .getOrCreate())
    
    spark.sparkContext.setLogLevel("WARN")
    print("=========================================================")
    print("=== PILAR 2: SPARK MLLIB (MACHINE LEARNING DISTRIBUIDO) ===")
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

    # Preparando o Vetor de Caracteristicas (Features)
    assembler = VectorAssembler(inputCols=["Sales"], outputCol="features")
    df_ml = assembler.transform(df)

    # Definindo a Variavel Alvo (Target ficticio: Meta = Sales * 1.1)
    df_ml = df_ml.withColumn("target", col("Sales") * 1.1)

    # Treinando a Regressao Linear em Memoria RAM
    lr = LinearRegression(featuresCol="features", labelCol="target")
    model = lr.fit(df_ml)

    print(f"\nModelo Treinado!")
    print(f"Coeficiente: {model.coefficients[0]:.2f} | Intercepto: {model.intercept:.2f}")

    print("\n--- Predicoes Geradas pelo Modelo ---")
    predictions = model.transform(df_ml)
    predictions.select("Person", "Sales", "prediction").show(5)

    spark.stop()

if __name__ == "__main__":
    main()