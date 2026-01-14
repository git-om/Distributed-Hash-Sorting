#!/bin/bash
# spark-env.sh

export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_MASTER_HOST=namenode
export SPARK_WORKER_CORES=4
export SPARK_WORKER_MEMORY=3g
export SPARK_EXECUTOR_MEMORY=2g
export SPARK_DRIVER_MEMORY=2g

# For better performance
export SPARK_LOCAL_DIRS=/tmp/spark
export SPARK_WORKER_DIR=/tmp/spark/work