import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

def main():
    if len(sys.argv) < 3:
        print("Uso: spark-submit --master yarn --deploy-mode client analise_logs.py <entrada_hdfs> <saida_hdfs> [--benchmark-q2]")
        sys.exit(1)

    entrada_hdfs = sys.argv[1]
    saida_hdfs = sys.argv[2].rstrip("/")
    modo_benchmark = "--benchmark-q2" in sys.argv

    spark = SparkSession.builder \
        .appName("AnaliseLogsWeb-Spark") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    try:
        esquema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("ip", StringType(), True),
            StructField("recurso", StringType(), True),
            StructField("status", IntegerType(), True),
            StructField("bytes", LongType(), True)
        ])

        df = spark.read.csv(entrada_hdfs, header=True, schema=esquema)

        # Modo exclusivo para medir desempenho da Q2 (sem gargalo de coalesce(1))
        if modo_benchmark:
            q2 = df.groupBy("ip").agg(F.count("*").cast("int").alias("acessos"))
            q2.write.mode("overwrite").option("sep", ",").csv(f"{saida_hdfs}/q2_bench")
            return

        # Q1 — Quantidade total de acessos
        total_acessos = df.count()
        df_q1 = spark.createDataFrame([(total_acessos,)], ["total"])
        df_q1.coalesce(1).write.mode("overwrite").csv(f"{saida_hdfs}/q1")

        # Q2 — Quantidade de acessos por IP
        df_q2 = df.groupBy("ip") \
                  .agg(F.count("*").cast("int").alias("acessos")) \
                  .orderBy(F.asc("ip"))
        df_q2.coalesce(1).write.mode("overwrite").option("sep", ",").csv(f"{saida_hdfs}/q2")

        # Q3 — Quantidade de acessos por status HTTP
        df_q3 = df.groupBy("status") \
                  .agg(F.count("*").cast("int").alias("acessos")) \
                  .orderBy(F.asc("status"))
        df_q3.coalesce(1).write.mode("overwrite").option("sep", ",").csv(f"{saida_hdfs}/q3")

        # Q4 — Soma dos bytes transferidos por recurso
        df_q4 = df.groupBy("recurso") \
                  .agg(F.sum("bytes").cast("long").alias("total_bytes")) \
                  .orderBy(F.asc("recurso"))
        df_q4.coalesce(1).write.mode("overwrite").option("sep", ",").csv(f"{saida_hdfs}/q4")

        # Q5 — Dez recursos mais acessados (contagem desc, desempate recurso asc)
        df_q5 = df.groupBy("recurso") \
                  .agg(F.count("*").cast("int").alias("acessos")) \
                  .orderBy(F.desc("acessos"), F.asc("recurso")) \
                  .limit(10)
        df_q5.coalesce(1).write.mode("overwrite").option("sep", ",").csv(f"{saida_hdfs}/q5")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
