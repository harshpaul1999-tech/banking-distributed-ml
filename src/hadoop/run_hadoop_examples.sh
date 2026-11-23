#!/usr/bin/env bash
# Run from the repository root on a Hadoop cluster with Python 3 on worker nodes.
set -euo pipefail

if [[ -z "${HADOOP_HOME:-}" ]]; then
  echo "Set HADOOP_HOME to a Hadoop installation with the streaming JAR." >&2
  exit 2
fi
DATA_PATH="${1:-/user/$USER/banking/input/bank.csv}"
OUTPUT_BASE="${2:-/user/$USER/banking/output}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
STREAMING_JAR="$(find "$HADOOP_HOME/share/hadoop/tools/lib" -name 'hadoop-streaming-*.jar' -print -quit)"
HDFS_DATA="/user/$USER/banking/input"

if [[ -z "$STREAMING_JAR" ]]; then
  echo "Could not find the Hadoop Streaming JAR under HADOOP_HOME." >&2
  exit 2
fi
hdfs dfs -mkdir -p "$HDFS_DATA"
hdfs dfs -put -f data/bank.csv "$HDFS_DATA/bank.csv"

run_job() {
  local name="$1" mapper="$2" reducer="$3"
  local output="$OUTPUT_BASE/$RUN_ID/$name"
  hadoop jar "$STREAMING_JAR" \
    -D "mapreduce.job.name=$name" \
    -files "src/hadoop/$mapper,src/hadoop/csv_rows.py,src/hadoop/$reducer" \
    -mapper "python3 $mapper" \
    -reducer "python3 $reducer" \
    -input "$DATA_PATH" \
    -output "$output"
  hdfs dfs -cat "$output/part-*"
}

run_job average_balance_by_job mr_average_balance_mapper.py mr_average_balance_reducer.py
run_job housing_counts_by_education mr_housing_by_education_mapper.py mr_count_reducer.py
run_job monthly_subscription_counts mr_month_subscription_mapper.py mr_count_reducer.py
run_job average_duration_by_outcome mr_average_duration_mapper.py mr_average_balance_reducer.py
run_job age_balance_summary mr_age_balance_mapper.py mr_age_balance_reducer.py
