# README — Hadoop MapReduce (Q1, Q2 e Q3)

## Pré-requisitos

- Q1.java, Q2.java, Q3.java e gerar_logs.py que precisam estar no diretório master.
- A semente utilizada foi 42 para a geração de logs.
- Critério de medição de tempo foi o REAL, impresso pelo time que inclui a inicialização do job, a leitura do HDFS e a escrita da saída.

## Como rodar

### Preparação do ambiente

```bash
./iniciar-cluster.sh
hdfs dfs -mkdir -p /dados/logs
hdfs dfs -put -f logs_exemplo.csv /dados/logs/
hdfs dfs -ls /dados/logs
mkdir -p classes
javac -classpath "$(hadoop classpath)" -d classes Q1.java Q2.java Q3.java
jar -cvf logs.jar -C classes .
```

### Validação

```bash
hdfs dfs -rm -r -f /user/aluno/saida_q1
hdfs dfs -rm -r -f /user/aluno/saida_q2
hdfs dfs -rm -r -f /user/aluno/saida_q3

hadoop jar logs.jar Q1 /dados/logs /user/aluno/saida_q1
hadoop jar logs.jar Q2 /dados/logs /user/aluno/saida_q2
hadoop jar logs.jar Q3 /dados/logs /user/aluno/saida_q3

hdfs dfs -cat /user/aluno/saida_q1/part-r-*
hdfs dfs -cat /user/aluno/saida_q2/part-r-*
hdfs dfs -cat /user/aluno/saida_q3/part-r-*
```

### Geração de logs/Criação de pastas no HDFS

```bash
python3 gerar_logs.py --registros 10000 --saida logs_10k.csv
python3 gerar_logs.py --registros 1000000 --saida logs_1m.csv
python3 gerar_logs.py --registros 10000000 --saida logs_10m.csv

hdfs dfs -mkdir -p /dados/logs_10k
hdfs dfs -mkdir -p /dados/logs_1m
hdfs dfs -mkdir -p /dados/logs_10m

hdfs dfs -put -f logs_1m.csv /dados/logs_1m/
hdfs dfs -put -f logs_10m.csv /dados/logs_10m/
hdfs dfs -put -f logs_10k.csv /dados/logs_10k/

hdfs dfs -rm -r -f /user/aluno/saida_q1_10k
hdfs dfs -rm -r -f /user/aluno/saida_q1_1m
hdfs dfs -rm -r -f /user/aluno/saida_q1_10m
hdfs dfs -rm -r -f /user/aluno/saida_q2_10k
hdfs dfs -rm -r -f /user/aluno/saida_q2_1m
hdfs dfs -rm -r -f /user/aluno/saida_q2_10m
hdfs dfs -rm -r -f /user/aluno/saida_q3_10k
hdfs dfs -rm -r -f /user/aluno/saida_q3_1m
hdfs dfs -rm -r -f /user/aluno/saida_q3_10m
hdfs dfs -rm -r -f /user/aluno/saida_q2_10kv2
hdfs dfs -rm -r -f /user/aluno/saida_q2_10kv3
hdfs dfs -rm -r -f /user/aluno/saida_q2_1mv2
hdfs dfs -rm -r -f /user/aluno/saida_q2_1mv3
hdfs dfs -rm -r -f /user/aluno/saida_q2_10mv2
hdfs dfs -rm -r -f /user/aluno/saida_q2_10mv3

time hadoop jar logs.jar Q1 /dados/logs_10k /user/aluno/saida_q1_10k
time hadoop jar logs.jar Q2 /dados/logs_10k /user/aluno/saida_q2_10k
time hadoop jar logs.jar Q2 /dados/logs_10k /user/aluno/saida_q2_10kv2
time hadoop jar logs.jar Q2 /dados/logs_10k /user/aluno/saida_q2_10kv3
time hadoop jar logs.jar Q3 /dados/logs_10k /user/aluno/saida_q3_10k

time hadoop jar logs.jar Q1 /dados/logs_1m /user/aluno/saida_q1_1m
time hadoop jar logs.jar Q2 /dados/logs_1m /user/aluno/saida_q2_1mv2
time hadoop jar logs.jar Q2 /dados/logs_1m /user/aluno/saida_q2_1mv3
time hadoop jar logs.jar Q2 /dados/logs_1m /user/aluno/saida_q2_1m
time hadoop jar logs.jar Q3 /dados/logs_1m /user/aluno/saida_q3_1m

time hadoop jar logs.jar Q1 /dados/logs_10m /user/aluno/saida_q1_10m
time hadoop jar logs.jar Q2 /dados/logs_10m /user/aluno/saida_q2_10m
time hadoop jar logs.jar Q2 /dados/logs_10m /user/aluno/saida_q2_10mv2
time hadoop jar logs.jar Q2 /dados/logs_10m /user/aluno/saida_q2_10mv3
time hadoop jar logs.jar Q3 /dados/logs_10m /user/aluno/saida_q3_10m
```
