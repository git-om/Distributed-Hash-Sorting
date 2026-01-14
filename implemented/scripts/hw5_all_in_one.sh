#!/usr/bin/env bash
# Usage:
#   ./scripts/hw5_all_in_one.sh small
#   ./scripts/hw5_all_in_one.sh large
#
# Run this on the *current* machine (either a small.instance or large.instance).
# It will:
#   - install deps (Java, Hadoop, Spark, Python libs)
#   - setup Hadoop + Spark (pseudo-distributed on this node)
#   - build your code (Maven + C++ hashgen/search)
#   - generate data with vaultx_linux_x86
#   - run your vaultx_hashgen (hashgen), HadoopVault, SparkVault
#   - dump logs into logs/

set -euo pipefail

ROLE="${1:-large}"    # "small" or "large"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$ROOT_DIR/logs"
BIN_DIR="$ROOT_DIR/bin"

mkdir -p "$LOG_DIR" "$BIN_DIR"

echo "==== HW5 all-in-one on this node (ROLE=$ROLE) ===="

########################################
# 0. Basic parameters per role
########################################
if [[ "$ROLE" == "small" ]]; then
  # Only 16GB (K=30) on small.instance with 2GB RAM
  K_LIST=("30")
  MEM_MB_VAULT=2048
  MEM_MB_HASHGEN=2048
elif [[ "$ROLE" == "large" ]]; then
  # Large instance can handle 16/32/64
  K_LIST=("30" "31" "32")
  MEM_MB_VAULT=16384
  MEM_MB_HASHGEN=16384
else
  echo "ROLE must be 'small' or 'large'"
  exit 1
fi

########################################
# 1. Install system deps
########################################
echo "==== Installing system dependencies (if needed) ===="
sudo apt-get update -y
sudo apt-get install -y \
  openjdk-17-jdk \
  build-essential \
  python3-pip \
  ssh rsync curl

pip3 install --user psutil pandas matplotlib

JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
export JAVA_HOME
export PATH="$JAVA_HOME/bin:$PATH"

########################################
# 2. Install Hadoop + Spark (single-node pseudo-distributed)
########################################
HADOOP_VERSION="3.4.2"
SPARK_VERSION="4.0.1"

if [[ ! -d /opt/hadoop ]]; then
  echo "==== Installing Hadoop $HADOOP_VERSION ===="
  cd /opt
  sudo curl -L -o "hadoop-${HADOOP_VERSION}.tar.gz" \
    "https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz"
  sudo tar -xzf "hadoop-${HADOOP_VERSION}.tar.gz"
  sudo ln -s "hadoop-${HADOOP_VERSION}" hadoop
fi

if [[ ! -d /opt/spark ]]; then
  echo "==== Installing Spark $SPARK_VERSION ===="
  cd /opt
  sudo curl -L -o "spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
    "https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz"
  sudo tar -xzf "spark-${SPARK_VERSION}-bin-hadoop3.tgz"
  sudo ln -s "spark-${SPARK_VERSION}-bin-hadoop3" spark
fi

export HADOOP_HOME=/opt/hadoop
export SPARK_HOME=/opt/spark
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$PATH"

echo "==== Configuring single-node Hadoop (replication=1) ===="
mkdir -p "$HOME/hw5-hadoop"
cd "$HADOOP_HOME/etc/hadoop"

# core-site.xml
cat > core-site.xml <<EOF
<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
EOF

# hdfs-site.xml
cat > hdfs-site.xml <<EOF
<?xml version="1.0"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:${HOME}/hw5-hadoop/name</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:${HOME}/hw5-hadoop/data</value>
  </property>
</configuration>
EOF

# yarn-site.xml
cat > yarn-site.xml <<EOF
<?xml version="1.0"?>
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>localhost</value>
  </property>
</configuration>
EOF

# mapred-site.xml
cp mapred-site.xml.template mapred-site.xml 2>/dev/null || true
cat > mapred-site.xml <<EOF
<?xml version="1.0"?>
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
EOF

echo "==== Formatting and starting HDFS/YARN (single-node) ===="
hdfs namenode -format -force > "$LOG_DIR/hdfs_format.log" 2>&1 || true
stop-dfs.sh >/dev/null 2>&1 || true
stop-yarn.sh >/dev/null 2>&1 || true
start-dfs.sh > "$LOG_DIR/start_dfs.log" 2>&1
start-yarn.sh > "$LOG_DIR/start_yarn.log" 2>&1

sleep 5
hdfs dfs -mkdir -p /hw5/input /hw5/output /hw5/spark || true

########################################
# 3. Spark basic env
########################################
cd "$SPARK_HOME/conf"
cp spark-env.sh.template spark-env.sh 2>/dev/null || true
cat > spark-env.sh <<EOF
export JAVA_HOME=${JAVA_HOME}
export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
EOF

########################################
# 4. Build your project (Java + C++)
########################################
cd "$ROOT_DIR"
echo "==== Building Maven jar ===="
mvn -q package

echo "==== Building hashgen/search binaries ===="
chmod +x "$ROOT_DIR/scripts/build_hashgen.sh"
"$ROOT_DIR/scripts/build_hashgen.sh"

########################################
# 5. Generate data & run experiments per K
########################################
VAULT_BIN="$ROOT_DIR/vaultx_linux_x86"
HASHGEN_BIN="$BIN_DIR/vaultx_hashgen"
SEARCHX_BIN="$BIN_DIR/searchx"
JAR_PATH="$(ls target/*.jar | head -n1)"

if [[ ! -x "$VAULT_BIN" ]]; then
  echo "ERROR: vaultx_linux_x86 not found/executable at $VAULT_BIN"
  echo "Place the professor's binary at that path."
  exit 1
fi

for K in "${K_LIST[@]}"; do
  echo "==== K=$K: generating dataset with vaultx_linux_x86 ===="
  DATA_BIN="data-k${K}.bin"
  DATA_TMP="data-k${K}.tmp"

  "$VAULT_BIN" -t 32 -i 1 -m "$MEM_MB_VAULT" -k "$K" \
      -g "$DATA_TMP" -f "$DATA_BIN" -d true \
      > "$LOG_DIR/vault_k${K}.log" 2>&1

  echo "==== K=$K: verifying vaultx data ===="
  "$VAULT_BIN" -m "$MEM_MB_VAULT" -k "$K" -f "$DATA_BIN" -v true \
      >> "$LOG_DIR/vault_k${K}.log" 2>&1

  ######################################
  # hashgen (your C++ vaultx.cpp)
  ######################################
  echo "==== K=$K: running your hashgen (vaultx_hashgen) ===="
  OUT_HASH_BIN="hashgen-k${K}.bin"
  "$HASHGEN_BIN" -t 32 -i 1 -m "$MEM_MB_HASHGEN" -k "$K" \
      -g "hashgen-k${K}.tmp" -f "$OUT_HASH_BIN" -v true -d true \
      > "$LOG_DIR/hashgen_k${K}.log" 2>&1

  ######################################
  # HadoopVault (hashgen+sort to HDFS)
  ######################################
  echo "==== K=$K: preparing Hadoop input file ===="
  GEN_FILE_LOCAL="$ROOT_DIR/gen-k${K}.txt"
  python3 - "$K" 64 "$GEN_FILE_LOCAL" <<'PYEOF'
import sys
k = int(sys.argv[1])
M = int(sys.argv[2])
out = sys.argv[3]
total = 1 << k
base = total // M
extra = total % M
with open(out,"w") as f:
    for i in range(M):
        n = base + (1 if i < extra else 0)
        f.write(str(n) + "\n")
PYEOF

  hdfs dfs -put -f "$GEN_FILE_LOCAL" "/hw5/input/gen-k${K}.txt"

  echo "==== K=$K: running HadoopVault job ===="
  hdfs dfs -rm -r "/hw5/output/hadoop-k${K}" >/dev/null 2>&1 || true
  hadoop jar "$JAR_PATH" \
      HadoopVault "$K" 64 \
      "/hw5/input/gen-k${K}.txt" \
      "/hw5/output/hadoop-k${K}" \
      > "$LOG_DIR/hadoop_k${K}.log" 2>&1

  ######################################
  # SparkVault (hashgen+sort via RDD)
  ######################################
  echo "==== K=$K: running SparkVault job ===="
  hdfs dfs -rm -r "/hw5/spark/output-k${K}" >/dev/null 2>&1 || true
  spark-submit \
      --class SparkVault \
      --master local[*] \
      "$JAR_PATH" \
      "$K" 64 "hdfs://localhost:9000/hw5/spark/output-k${K}" \
      > "$LOG_DIR/spark_k${K}.log" 2>&1

  ######################################
  # Search experiments: hashgen & vaultx
  ######################################
  # hashgen search
  echo "==== K=$K: hashgen search (difficulty 3 & 4) ===="
  "$SEARCHX_BIN" -k "$K" -f "$OUT_HASH_BIN" -s 1000 -q 3 -d true \
      > "$LOG_DIR/search_hashgen_k${K}_q3.log" 2>&1
  "$SEARCHX_BIN" -k "$K" -f "$OUT_HASH_BIN" -s 1000 -q 4 -d true \
      > "$LOG_DIR/search_hashgen_k${K}_q4.log" 2>&1

  # vaultx search
  echo "==== K=$K: vaultx search (difficulty 3 & 4) ===="
  "$VAULT_BIN" -k "$K" -f "$DATA_BIN" -s 1000 -q 3 -d true \
      > "$LOG_DIR/search_vaultx_k${K}_q3.log" 2>&1
  "$VAULT_BIN" -k "$K" -f "$DATA_BIN" -s 1000 -q 4 -d true \
      > "$LOG_DIR/search_vaultx_k${K}_q4.log" 2>&1

done

echo
echo "==== DONE on this node (ROLE=$ROLE) ===="
echo "Logs are in: $LOG_DIR"
echo "Examples:"
echo "  vaultx (prof) logs : $LOG_DIR/vault_k30.log ..."
echo "  your hashgen logs  : $LOG_DIR/hashgen_k30.log ..."
echo "  Hadoop logs        : $LOG_DIR/hadoop_k30.log ..."
echo "  Spark logs         : $LOG_DIR/spark_k30.log ..."
echo "  search logs        : $LOG_DIR/search_hashgen_k30_q3.log, search_vaultx_k30_q3.log, etc."
