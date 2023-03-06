#!/usr/bin/env bash

. ./functions.sh
. ./benchmark-env.sh

welcome
set_java_command
validate_flink_home

echo "1) Run Kafka datasource"
echo "2) Run Benchmark"
echo "3) Run Kafka result analyzer"

echo -e "\n"

read -p "Please input your choice(0 to exit): " CHOICE

display_path

case "$CHOICE" in
"1")
  run_kafka_datagen
  ;;

"2")
  run_benchmark
  ;;

"3")
  run_kafka_result_analyzer
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
