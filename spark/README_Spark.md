# README — Apache Spark (Q1, Q2, Q3, Q4 e Q5)

## Pré-requisitos

- `analise_logs.py` e `gerar_logs.py` precisam estar no diretório do container `master`.
- A semente utilizada foi `42` para a geração de logs (500 IPs distintos nos experimentos e 12 IPs em `logs_exemplo.csv`).
- O critério de medição de tempo foi o `real`, impresso pelo comando `time`, que inclui a inicialização da sessão Spark e dos executores, a leitura do HDFS, o processamento e a escrita da saída no HDFS.

## Como rodar

### Preparação do ambiente

```bash
./iniciar-cluster.sh
hdfs dfs -mkdir -p /dados/logs
hdfs dfs -put -f logs_exemplo.csv /dados/logs/
hdfs dfs -ls /dados/logs
```

### Validação

```bash
hdfs dfs -rm -r -f /user/aluno/saida_spark_exemplo

spark-submit --master spark://master:7077 --deploy-mode client analise_logs.py \
  /dados/logs/logs_exemplo.csv /user/aluno/saida_spark_exemplo

hdfs dfs -cat /user/aluno/saida_spark_exemplo/q1/part-*
hdfs dfs -cat /user/aluno/saida_spark_exemplo/q2/part-*
hdfs dfs -cat /user/aluno/saida_spark_exemplo/q3/part-*
hdfs dfs -cat /user/aluno/saida_spark_exemplo/q4/part-*
hdfs dfs -cat /user/aluno/saida_spark_exemplo/q5/part-*
```

### Geração de logs e criação de pastas no HDFS

```bash
python3 gerar_logs.py --registros 10000 --saida logs_10k.csv
python3 gerar_logs.py --registros 1000000 --saida logs_1m.csv
python3 gerar_logs.py --registros 10000000 --saida logs_10m.csv

hdfs dfs -mkdir -p /dados/logs_10k
hdfs dfs -mkdir -p /dados/logs_1m
hdfs dfs -mkdir -p /dados/logs_10m

hdfs dfs -put -f logs_10k.csv /dados/logs_10k/
hdfs dfs -put -f logs_1m.csv /dados/logs_1m/
hdfs dfs -put -f logs_10m.csv /dados/logs_10m/
```

### Experimentos de desempenho por volume (Q2)

```bash
hdfs dfs -rm -r -f /user/aluno/saida_spark_q2_10k
hdfs dfs -rm -r -f /user/aluno/saida_spark_q2_10kv2
hdfs dfs -rm -r -f /user/aluno/saida_spark_q2_10kv3
hdfs dfs -rm -r -f /user/aluno/saida_spark_q2_1m
hdfs dfs -rm -r -f /user/aluno/saida_spark_q2_1mv2
hdfs dfs -rm -r -f /user/aluno/saida_spark_q2_1mv3
hdfs dfs -rm -r -f /user/aluno/saida_spark_q2_10m
hdfs dfs -rm -r -f /user/aluno/saida_spark_q2_10mv2
hdfs dfs -rm -r -f /user/aluno/saida_spark_q2_10mv3

time spark-submit --master spark://master:7077 --deploy-mode client analise_logs.py /dados/logs_10k /user/aluno/saida_spark_q2_10k --benchmark-q2
time spark-submit --master spark://master:7077 --deploy-mode client analise_logs.py /dados/logs_10k /user/aluno/saida_spark_q2_10kv2 --benchmark-q2
time spark-submit --master spark://master:7077 --deploy-mode client analise_logs.py /dados/logs_10k /user/aluno/saida_spark_q2_10kv3 --benchmark-q2

time spark-submit --master spark://master:7077 --deploy-mode client analise_logs.py /dados/logs_1m /user/aluno/saida_spark_q2_1m --benchmark-q2
time spark-submit --master spark://master:7077 --deploy-mode client analise_logs.py /dados/logs_1m /user/aluno/saida_spark_q2_1mv2 --benchmark-q2
time spark-submit --master spark://master:7077 --deploy-mode client analise_logs.py /dados/logs_1m /user/aluno/saida_spark_q2_1mv3 --benchmark-q2

time spark-submit --master spark://master:7077 --deploy-mode client analise_logs.py /dados/logs_10m /user/aluno/saida_spark_q2_10m --benchmark-q2
time spark-submit --master spark://master:7077 --deploy-mode client analise_logs.py /dados/logs_10m /user/aluno/saida_spark_q2_10mv2 --benchmark-q2
time spark-submit --master spark://master:7077 --deploy-mode client analise_logs.py /dados/logs_10m /user/aluno/saida_spark_q2_10mv3 --benchmark-q2
```

### Experimento de variação de paralelismo (10 milhões de registros)

```bash
hdfs dfs -rm -r -f /user/aluno/saida_spark_p1_v1 /user/aluno/saida_spark_p1_v2 /user/aluno/saida_spark_p1_v3
hdfs dfs -rm -r -f /user/aluno/saida_spark_p2_v1 /user/aluno/saida_spark_p2_v2 /user/aluno/saida_spark_p2_v3
hdfs dfs -rm -r -f /user/aluno/saida_spark_p4_v1 /user/aluno/saida_spark_p4_v2 /user/aluno/saida_spark_p4_v3

# 1 núcleo
time spark-submit --master spark://master:7077 --deploy-mode client --total-executor-cores 1 analise_logs.py /dados/logs_10m /user/aluno/saida_spark_p1_v1 --benchmark-q2
time spark-submit --master spark://master:7077 --deploy-mode client --total-executor-cores 1 analise_logs.py /dados/logs_10m /user/aluno/saida_spark_p1_v2 --benchmark-q2
time spark-submit --master spark://master:7077 --deploy-mode client --total-executor-cores 1 analise_logs.py /dados/logs_10m /user/aluno/saida_spark_p1_v3 --benchmark-q2

# 2 núcleos
time spark-submit --master spark://master:7077 --deploy-mode client --total-executor-cores 2 analise_logs.py /dados/logs_10m /user/aluno/saida_spark_p2_v1 --benchmark-q2
time spark-submit --master spark://master:7077 --deploy-mode client --total-executor-cores 2 analise_logs.py /dados/logs_10m /user/aluno/saida_spark_p2_v2 --benchmark-q2
time spark-submit --master spark://master:7077 --deploy-mode client --total-executor-cores 2 analise_logs.py /dados/logs_10m /user/aluno/saida_spark_p2_v3 --benchmark-q2

# 4 núcleos
time spark-submit --master spark://master:7077 --deploy-mode client --total-executor-cores 4 analise_logs.py /dados/logs_10m /user/aluno/saida_spark_p4_v1 --benchmark-q2
time spark-submit --master spark://master:7077 --deploy-mode client --total-executor-cores 4 analise_logs.py /dados/logs_10m /user/aluno/saida_spark_p4_v2 --benchmark-q2
time spark-submit --master spark://master:7077 --deploy-mode client --total-executor-cores 4 analise_logs.py /dados/logs_10m /user/aluno/saida_spark_p4_v3 --benchmark-q2
```