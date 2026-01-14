#!/usr/bin/env bash
# Usage:
#   ./scripts/hw5_run.sh small   # run experiments appropriate for 1 small.instance (only 16GB)
#   ./scripts/hw5_run.sh large   # run experiments for 1 large.instance (16/32/64GB)
#
# This script assumes:
#   - Hadoop is installed and HDFS is running.
#   - Spark is installed (spark-submit works).
#   - vaultx_linux_x86 (prof binary) is at repo root and is executable.
#   - Maven is installed.
#
# It will:
#   - Build your jar and C++ binaries
#   - For each K:
#       * generate data with vaultx_linux_x86
#       * run your hashgen (bin/vaultx)
#       * generate Hadoop input file and run HadoopVault
#       * run SparkVault
#       * run hashgen + vaultx search (difficulty 3 and 4)
#   - Put logs in ./logs/

set -euo pipefail

ROLE="${1:-large}"   # default to 'large' if not provided
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$ROOT_DIR/logs"
BIN_DIR="$ROOT_DIR/bin"

mkdir -p "$LOG_DIR" "$BIN_DIR"

echo "==== HW5 run on this node (ROLE=$ROLE) ===="

########################################
# 0. K values and memory per role
########################################
if [[ "$ROLE" == "small" ]]; then
  # Only 16GB (K=30) on small.instance, per your note
  K_LIST=("30")
  MEM_MB_VAULT=2048        # 2GB RAM for vaultx_linux_x86
  MEM_MB_HASHGEN=2048      # 2GB RAM for your C++ hashgen
elif [[ "$ROLE" == "large" ]]; then
  # Large instance can handle 16/32/64GB
  K_LIST=("30" "31" "32")
  MEM_MB_VAULT=16384       # 16GB RAM
  MEM_MB_HASHGEN=16384
else
  echo "ROLE must be 'small' or 'large'"
  exit 1
fi

########################################
# 1. Build Java + C++ once
########################################
cd "$ROOT_DIR"

echo "==== Building Maven jar ===="
mvn -q package

JAR_PATH="$(ls target/*.jar | head -n1)"
echo "Using jar: $JAR_PATH"

echo "==== Building C++ hashgen + searchx ===="
chmod +x "$ROOT_DIR/scripts/build_hashgen.sh"
"$ROOT_DIR/scripts/build_hashgen.sh"

VAULT_PROF="$ROOT_DIR/vaultx_linux_x86"
VAULT_YOURS="$BIN_DIR/vaultx"
SEARCHX_BIN="$BIN_DIR/searchx"

if [[ ! -x "$VAULT_PROF" ]]; then
  echo "ERROR: vaultx_linux_x86 not found at $VAULT_PROF or not executable"
  echo "Place the professor's binary at repo root and chmod +x it."
  exit 1
fi

########################################
# 2. Make sure HDFS dirs exist
########################################
hdfs dfs -mkdir -p /hw5/input /hw5/output /hw5/spark || true

########################################
# 3. Loop over k and run experiments
########################################
for K in "${K_LIST[@]}"; do
  echo
  echo "============================="
  echo "==== Running K=$K tasks ===="
  echo "============================="

  DATA_BIN="data-k${K}.bin"
  DATA_TMP="data-k${K}.tmp"

  ######################################
  # 3.1 Generate data with prof's vaultx
  ######################################
  echo "---- [K=$K] vaultx_linux_x86 (prof) generating data ----"
  "$VAULT_PROF" -t 32 -i 1 -m "$MEM_MB_VAULT" -k "$K" \
      -g "$DATA_TMP" -f "$DATA_BIN" -d true \
      > "$LOG_DIR/vault_k${K}.log" 2>&1

  # Verify
  "$VAULT_PROF" -m "$MEM_MB_VAULT" -k "$K" -f "$DATA_BIN" -v true \
      >> "$LOG_DIR/vault_k${K}.log" 2>&1

  # For K=32 on large, copy log to vault64GB.log for submission
  if [[ "$ROLE" == "large" && "$K" == "32" ]]; then
    cp "$LOG_DIR/vault_k${K}.log" "$LOG_DIR/vault64GB.log"
  fi

  ######################################
  # 3.2 Your hashgen (C++ vaultx.cpp)
  ######################################
  echo "---- [K=$K] your hashgen (bin/vaultx) ----"
  HASH_BIN="hashgen-k${K}.bin"

  "$VAULT_YOURS" -t 32 -i 1 -m "$MEM_MB_HASHGEN" -k "$K" \
      -g "hashgen-k${K}.tmp" -f "$HASH_BIN" -v true -d true \
      > "$LOG_DIR/hashgen_k${K}.log" 2>&1

  if [[ "$ROLE" == "large" && "$K" == "32" ]]; then
    cp "$LOG_DIR/hashgen_k${K}.log" "$LOG_DIR/hashgen64GB.log"
  fi

  ######################################
  # 3.3 HadoopVault: hashgen+sort to HDFS
  ######################################
  echo "---- [K=$K] Preparing Hadoop input file ----"
  GEN_LOCAL="$ROOT_DIR/gen-k${K}.txt"
  python3 "$ROOT_DIR/scripts/make_hadoop_input.py" "$K" 64 "$GEN_LOCAL"

  hdfs dfs -put -f "$GEN_LOCAL" "/hw5/input/gen-k${K}.txt"

  echo "---- [K=$K] Running HadoopVault job ----"
  hdfs dfs -rm -r "/hw5/output/hadoop-k${K}" >/dev/null 2>&1 || true

  hadoop jar "$JAR_PATH" \
      HadoopVault "$K" 64 \
      "/hw5/input/gen-k${K}.txt" \
      "/hw5/output/hadoop-k${K}" \
      > "$LOG_DIR/hadoop_k${K}.log" 2>&1

  if [[ "$ROLE" == "large" && "$K" == "32" ]]; then
    cp "$LOG_DIR/hadoop_k${K}.log" "$LOG_DIR/hadoop64GB.log"
  fi

  ######################################
  # 3.4 SparkVault: hashgen+sort via Spark
  ######################################
  echo "---- [K=$K] Running SparkVault job ----"
  hdfs dfs -rm -r "/hw5/spark/output-k${K}" >/dev/null 2>&1 || true

  # Adjust --master as needed (local[*] is fine for 1-node experiments)
  spark-submit \
      --class SparkVault \
      --master local[*] \
      "$JAR_PATH" \
      "$K" 64 "hdfs://localhost:9000/hw5/spark/output-k${K}" \
      > "$LOG_DIR/spark_k${K}.log" 2>&1

  if [[ "$ROLE" == "large" && "$K" == "32" ]]; then
    cp "$LOG_DIR/spark_k${K}.log" "$LOG_DIR/spark64GB.log"
  fi

  ######################################
  # 3.5 Search experiments (hashgen + vaultx)
  ######################################
  echo "---- [K=$K] hashgen search (difficulty 3 & 4) ----"
  "$SEARCHX_BIN" -k "$K" -f "$HASH_BIN" -s 1000 -q 3 -d true \
      > "$LOG_DIR/search_hashgen_k${K}_q3.log" 2>&1
  "$SEARCHX_BIN" -k "$K" -f "$HASH_BIN" -s 1000 -q 4 -d true \
      > "$LOG_DIR/search_hashgen_k${K}_q4.log" 2>&1

  echo "---- [K=$K] vaultx search (difficulty 3 & 4) ----"
  "$VAULT_PROF" -k "$K" -f "$DATA_BIN" -s 1000 -q 3 -d true \
      > "$LOG_DIR/search_vaultx_k${K}_q3.log" 2>&1
  "$VAULT_PROF" -k "$K" -f "$DATA_BIN" -s 1000 -q 4 -d true \
      > "$LOG_DIR/search_vaultx_k${K}_q4.log" 2>&1

done

echo
echo "==== ALL DONE for ROLE=$ROLE ===="
echo "Logs are in: $LOG_DIR"
echo "Key files for the report (on large, K=32):"
echo "  hashgen64GB.log  (your hashgen)"
echo "  vault64GB.log    (prof's vaultx)"
echo "  hadoop64GB.log   (HadoopVault)"
echo "  spark64GB.log    (SparkVault)"
