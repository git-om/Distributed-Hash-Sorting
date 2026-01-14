#!/bin/bash
# run_experiments.sh - Main script to run all experiments

set -e

# Configuration
K_VALUES=(30 31 32)
HADOOP_REDUCERS=8
SPARK_PARTITIONS=8

# Function to run hashgen
run_hashgen() {
    local k=$1
    local output=$2
    local threads=$3
    local memory=$4
    
    echo "=== Running hashgen: k=$k, threads=$threads, memory=${memory}GB ==="
    python3 hashgen.py -k $k -f $output -t $threads 2>&1 | tee hashgen_k${k}.log
}

# Function to run vaultx
run_vaultx() {
    local k=$1
    local output=$2
    local threads=$3
    local memory=$4
    
    echo "=== Running vaultx: k=$k, threads=$threads, memory=${memory}GB ==="
    ./vaultx -t $threads -i 1 -m $memory -k $k -g ${output}.tmp -f $output 2>&1 | tee vault_k${k}.log
    ./vaultx -f $output -v true
}

# Function to run Hadoop
run_hadoop() {
    local k=$1
    local output=$2
    
    echo "=== Running Hadoop: k=$k ==="
    
    # Create input file for mappers
    echo "$((1 << k))" > /tmp/hadoop_input.txt
    hdfs dfs -rm -r -f /input /output
    hdfs dfs -mkdir -p /input
    hdfs dfs -put /tmp/hadoop_input.txt /input/
    
    # Run Hadoop job
    hadoop jar hw5-vault-1.0.jar HadoopVault $k $HADOOP_REDUCERS /input /output 2>&1 | tee hadoop_k${k}.log
    
    # Copy output back
    hdfs dfs -get /output $output
}

# Function to run Spark
run_spark() {
    local k=$1
    local output=$2
    
    echo "=== Running Spark: k=$k ==="
    
    hdfs dfs -rm -r -f /spark_output
    
    # Run Spark job
    spark-submit \
        --class SparkVault \
        --master yarn \
        --deploy-mode cluster \
        --executor-memory 2G \
        --num-executors $SPARK_PARTITIONS \
        hw5-vault-1.0.jar $k $SPARK_PARTITIONS hdfs:///spark_output 2>&1 | tee spark_k${k}.log
    
    # Copy output back
    hdfs dfs -get /spark_output $output
}

# Function to monitor resources
monitor_resources() {
    local output=$1
    local pid=$2
    
    echo "timestamp,cpu_percent,mem_gb,disk_read_mb,disk_write_mb" > $output
    
    while kill -0 $pid 2>/dev/null; do
        timestamp=$(date +%s)
        cpu=$(ps -p $pid -o %cpu | tail -1)
        mem=$(ps -p $pid -o rss | tail -1)
        mem_gb=$(echo "scale=2; $mem / 1024 / 1024" | bc)
        
        disk_stats=$(iostat -d -k 1 1 | grep -A1 sda | tail -1)
        disk_read=$(echo $disk_stats | awk '{print $3/1024}')
        disk_write=$(echo $disk_stats | awk '{print $4/1024}')
        
        echo "$timestamp,$cpu,$mem_gb,$disk_read,$disk_write" >> $output
        sleep 1
    done
}

# Experiment 1: Small instance experiments
echo "=== Experiment Set 1: Small Instance (4 cores, 4GB RAM) ==="
for k in "${K_VALUES[@]}"; do
    if [ $k -lt 32 ]; then
        run_hashgen $k "data-${k}GB.bin" 4 2
        run_vaultx $k "data-${k}GB-vault.bin" 4 2048
    fi
done

# Experiment 2: Large instance experiments
echo "=== Experiment Set 2: Large Instance (32 cores, 32GB RAM) ==="
for k in "${K_VALUES[@]}"; do
    run_hashgen $k "data-${k}GB-large.bin" 32 16
    run_vaultx $k "data-${k}GB-large-vault.bin" 32 16384
done

# Experiment 3: Hadoop on 8 small instances
echo "=== Experiment Set 3: Hadoop on 8 Small Instances ==="
for k in "${K_VALUES[@]}"; do
    run_hadoop $k "hadoop_output_k${k}"
done

# Experiment 4: Spark on 8 small instances
echo "=== Experiment Set 4: Spark on 8 Small Instances ==="
for k in "${K_VALUES[@]}"; do
    run_spark $k "spark_output_k${k}"
done

echo "=== All experiments complete ==="