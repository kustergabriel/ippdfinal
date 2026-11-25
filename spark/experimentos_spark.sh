set -euo pipefail

HDFS_DADOS="hdfs:///user/aluno/dados-logs"
SCRIPT_SPARK="/home/aluno/trabalho-spark/analise_logs.py"
ARQ_CSV="/home/aluno/trabalho-spark/resultados_spark.csv"

echo "Experimento,Parametro,Execucao,Tempo_Segundos" > "$ARQ_CSV"

echo ">>> Iniciando Experimentos de Volume (10k, 1M, 10M)..."
for BASE in logs_10k.csv logs_1m.csv logs_10m.csv; do
    for RUN in 1 2 3; do
        SAIDA="hdfs:///user/aluno/bench-spark-${BASE}-${RUN}"
        hdfs dfs -rm -r -f "$SAIDA" >/dev/null 2>&1 || true
        
        INICIO=$(date +%s.%N)
        spark-submit --master yarn --deploy-mode client "$SCRIPT_SPARK" \
            "${HDFS_DADOS}/${BASE}" "$SAIDA" --benchmark-q2 >/dev/null 2>&1
        FIM=$(date +%s.%N)
        
        TEMPO=$(python3 -c "print(f'{float($FIM) - float($INICIO):.4f}')")
        echo "Volume,$BASE,$RUN,$TEMPO" >> "$ARQ_CSV"
        echo "[Volume] $BASE | Run $RUN: ${TEMPO}s"
    done
done

echo ">>> Iniciando Experimentos de Paralelismo (1, 2 e 4 executores em 10M)..."
for CORES in 1 2 4; do
    for RUN in 1 2 3; do
        SAIDA="hdfs:///user/aluno/bench-spark-paralelismo-${CORES}-${RUN}"
        hdfs dfs -rm -r -f "$SAIDA" >/dev/null 2>&1 || true
        
        INICIO=$(date +%s.%N)
        spark-submit --master yarn --deploy-mode client \
            --num-executors "$CORES" --executor-cores 1 \
            "$SCRIPT_SPARK" "${HDFS_DADOS}/logs_10m.csv" "$SAIDA" --benchmark-q2 >/dev/null 2>&1
        FIM=$(date +%s.%N)
        
        TEMPO=$(python3 -c "print(f'{float($FIM) - float($INICIO):.4f}')")
        echo "Paralelismo_10M,${CORES}_executores,$RUN,$TEMPO" >> "$ARQ_CSV"
        echo "[Paralelismo 10M] $CORES executor(es) | Run $RUN: ${TEMPO}s"
    done
done

echo "Concluido! Resultados salvos em $ARQ_CSV"
cat "$ARQ_CSV"
