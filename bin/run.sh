#!/usr/bin/env bash

. ./functions.sh
. ./benchmark-env.sh

welcome
set_java_command
validate_flink_home

echo "1) Run Kafka datasource"
echo "2) Run Benchmark"
echo "3) Run Kafka latency analyzer"

echo -e "\n"

read -p "Please input your choice(0 to exit): " CHOICE

case "$CHOICE" in
"1")
  run_kafka_datasource
  ;;

"2")
  run_benchmark
  ;;

"3")
  run_kafka_latency_analyzer
  ;;

"0")
  echo "Bye"
  exit 0
  ;;

*)
  echo "Invalid choice"
  exit -1
  ;;

esac
