#!/bin/bash
# run_search_experiments.sh - Run all search experiments

set -e

K_VALUES=(30 31 32)
DIFFICULTIES=(3 4)
NUM_SEARCHES=1000

echo "=== Running Search Experiments ==="

# Hashgen searches
echo "=== Hashgen Searches ==="
for k in "${K_VALUES[@]}"; do
    for diff in "${DIFFICULTIES[@]}"; do
        echo "Running hashgen search: k=$k, difficulty=$diff"
        python3 hashgen.py -k $k -f "data-${k}GB-large.bin" -s $NUM_SEARCHES -q $diff \
            2>&1 | tee "search_hashgen_k${k}_d${diff}.log"
    done
done

# Vaultx searches
echo "=== Vaultx Searches ==="
for k in "${K_VALUES[@]}"; do
    for diff in "${DIFFICULTIES[@]}"; do
        echo "Running vaultx search: k=$k, difficulty=$diff"
        ./vaultx -k $k -f "data-${k}GB-large-vault.bin" -s $NUM_SEARCHES -q $diff -d true \
            2>&1 | tee "search_vault_k${k}_d${diff}.log"
    done
done

# Hadoop searches (requires custom search implementation)
echo "=== Hadoop Searches ==="
for k in "${K_VALUES[@]}"; do
    for diff in "${DIFFICULTIES[@]}"; do
        echo "Running Hadoop search: k=$k, difficulty=$diff"
        hadoop jar hw5-vault-1.0.jar HadoopSearch $k $diff $NUM_SEARCHES \
            hdfs:///output "search_results_k${k}_d${diff}" \
            2>&1 | tee "search_hadoop_k${k}_d${diff}.log"
    done
done

# Spark searches
echo "=== Spark Searches ==="
for k in "${K_VALUES[@]}"; do
    for diff in "${DIFFICULTIES[@]}"; do
        echo "Running Spark search: k=$k, difficulty=$diff"
        spark-submit --class SparkSearch --master yarn \
            hw5-vault-1.0.jar $k $diff $NUM_SEARCHES hdfs:///spark_output \
            2>&1 | tee "search_spark_k${k}_d${diff}.log"
    done
done

echo "=== Search experiments complete ==="