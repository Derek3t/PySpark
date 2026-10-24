import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = (SparkSession.builder
             .appName("Modulo_4_StructuredStreaming")
             .master("local[*]")
             .getOrCreate())
    
    spark.sparkContext.setLogLevel("WARN")
    print("=========================================================")
    print("=== PILAR 4: STRUCTURED STREAMING (TEMPO REAL)        ===")
    print("=========================================================")
    print("Simulando ingestao continua de dados por 5 segundos...")

    # Fonte em Tempo Real (Gerador 'rate' simulando 100 eventos/segundo)
    stream_df = (spark.readStream
                 .format("rate")
                 .option("rowsPerSecond", 10)
                 .load()
                 .withColumn("valor_transacao", (col("value") * 37) % 1000))

    # Regra de Negocio: Filtrar apenas transacoes com valor alto (Alertas)
    stream_alertas = stream_df.filter(col("valor_transacao") > 800)

    # Escrevendo o resultado de forma continua em uma tabela na memoria
    query_stream = (stream_alertas.writeStream
                    .queryName("alertas_streaming")
                    .format("memory")
                    .outputMode("append")
                    .start())

    # Aguarda 5 segundos acumulando mensagens do stream
    time.sleep(5)

    print("\n--- Consultando a Tabela Infinita gerada pelo Streaming ---")
    spark.sql("SELECT timestamp, valor_transacao FROM alertas_streaming LIMIT 5").show()

    # Interrompe a consulta streaming
    query_stream.stop()
    spark.stop()

if __name__ == "__main__":
    main()