# Análise de Logs Web com Hadoop MapReduce e Apache Spark

Trabalho final de Introdução ao Processamento Paralelo e Distribuído (IPPD).
Os mesmos logs de acesso web são processados com **Hadoop MapReduce** (Java) e com **Apache Spark** (PySpark), e o desempenho das duas soluções é comparado.

## Formato dos dados

Arquivos CSV com cabeçalho, gerados pelo `gerar_logs.py` com semente `42`:

```
timestamp,ip,recurso,status,bytes
```

Bases utilizadas: `logs_exemplo.csv` (validação), `logs_10k.csv`, `logs_1m.csv` e `logs_10m.csv`.

## Consultas

| Consulta | Descrição                                   | Hadoop    | Spark |
|----------|---------------------------------------------|-----------|-------|
| Q1       | Quantidade total de acessos                 | `Q1.java` | ✔     |
| Q2       | Quantidade de acessos por IP                | `Q2.java` | ✔     |
| Q3       | Quantidade de acessos por status HTTP       | `Q3.java` | ✔     |
| Q4       | Soma dos bytes transferidos por recurso     | —         | ✔     |
| Q5       | Dez recursos mais acessados                 | —         | ✔     |

## Estrutura

```
.
├── hadoop/
│   ├── Q1.java, Q2.java, Q3.java   # jobs MapReduce
│   └── README.md                   # como compilar, rodar e medir
└── spark/
    ├── analise_logs.py             # Q1–Q5 em PySpark (--benchmark-q2 roda só a Q2)
    ├── experimentos_spark.sh       # automatiza os experimentos e gera um CSV de tempos
    └── README_Spark.md             # como rodar e medir
```

## Experimentos

Feitos com a **Q2** (acessos por IP), 3 execuções cada:

- **Volume:** bases de 10 mil, 1 milhão e 10 milhões de registros.
- **Paralelismo:** base de 10 milhões com 1, 2 e 4 map tasks (Hadoop) ou núcleos/executores (Spark).

O tempo medido é o `real` do comando `time`, que inclui a inicialização do job, a leitura do HDFS, o processamento e a escrita da saída.

## Como rodar (resumo)

```bash
# Hadoop
javac -classpath "$(hadoop classpath)" -d classes Q1.java Q2.java Q3.java
jar -cvf logs.jar -C classes .
hadoop jar logs.jar Q2 /dados/logs /user/aluno/saida_q2

# Spark
spark-submit --master spark://master:7077 --deploy-mode client analise_logs.py \
  /dados/logs/logs_exemplo.csv /user/aluno/saida_spark_exemplo
```

Os passos completos estão em [`hadoop/README.md`](hadoop/README.md) e [`spark/README_Spark.md`](spark/README_Spark.md).

## Licença

[MIT](LICENSE)
