#!/usr/bin/env bash

FLINK_JOB_JAR_NAME="benchmark-*.jar"
KAFKA_DATAGEN_JAR_NAME="kafka-datagen-*.jar"
KAFKA_LATENCY_ANALYZER_JAR_NAME="kafka-latency-analyzer-*.jar"

FLINK_JOB_JAR_PATH=$(readlink -f ../"${FLINK_JOB_JAR_NAME}")
KAFKA_DATAGEN_JAR_PATH=$(readlink -f ../"${KAFKA_DATAGEN_JAR_NAME}")
KAFKA_LATENCY_ANALYZER_JAR_PATH=$(readlink -f ../"${KAFKA_LATENCY_ANALYZER_JAR_NAME}")

welcome() {
  echo "=============================================="
  echo "======                                  ======"
  echo "======  Flink benchmark project v1.0    ======"
  echo "======      Author: Paul Zhang          ======"
  echo "======                                  ======"
  echo "=============================================="
  echo -e "\n"
}

display_path() {
  echo 'Found jar files location:'
  echo "${FLINK_JOB_JAR_PATH}"
  echo "${KAFKA_DATAGEN_JAR_PATH}"
  echo "${KAFKA_LATENCY_ANALYZER_JAR_PATH}"
}

set_java_command() {
  if [[ $(type java >/dev/null) -eq 0 ]]; then
    JAVA_COMMAND="java"
  elif [[ -e $JAVA_HOME ]]; then
    JAVA_COMMAND=${JAVA_HOME}/bin/java
  fi

  if [[ -z "$JAVA_COMMAND" ]]; then
    echo "Error: Cannot locate Java. Abort."
    exit -1
  fi
}

validate_flink_home() {
  if [[ ! -e $FLINK_HOME ]]; then
    echo "Error: Cannot find flink command. Abort"
    exit -1
  fi
}

check_jar_file_exists() {
  if [[ ! -e $1 ]]; then
    echo "Cannot locate jar file in: $1"
    exit -1
  fi
}

run_kafka_datagen() {
  echo "Run Kafka datagen"

  check_jar_file_exists "$KAFKA_DATAGEN_JAR_PATH"

  read -rp "Kafka bootstrap server: " BOOTSTRAP_SERVER
  read -rp "Input topic: " TOPIC
  read -rp "Acks: " ACKS
  read -rp "Number of threads: " THREADS
  read -rp "Message send interval: " INTERVAL
  read -rp "Payload type(uuid/1kb): " PAYLOAD_TYPE

  $JAVA_COMMAND -jar "$KAFKA_DATAGEN_JAR_PATH" -b "$BOOTSTRAP_SERVER" -n "$THREADS" -a "$ACKS" -t "$TOPIC" -i "$INTERVAL" -p "$PAYLOAD_TYPE"
}

run_kafka_latency_analyzer() {
  echo "Run Kafka latency analyzer"

  check_jar_file_exists "$KAFKA_LATENCY_ANALYZER_JAR_PATH"

  read -rp "Kafka bootstrap server: " BOOTSTRAP_SERVER
  read -rp "Output topic: " TOPIC

  $JAVA_COMMAND -jar "$KAFKA_LATENCY_ANALYZER_JAR_PATH" -b "$BOOTSTRAP_SERVER" -t "$TOPIC"
}

run_benchmark() {
  echo "Run benchmark"

  check_jar_file_exists "$FLINK_JOB_JAR_PATH"
  HADOOP_CLASSPATH=$(hadoop classpath)
  export HADOOP_CLASSPATH

  echo "Type of benchmark:"
  echo "1) Throughput"
  echo "2) Latency"

  read -rp "Your choice(0 to exit): " CHOICE
  case "$CHOICE" in
  "1")
    FLINK_CLASS="com.paultech.Throughput"
    read -rp "kafka bootstrap server: " BOOTSTRAP_SERVER
    read -rp "Input topic: " INPUT_TOPIC
    read -rp "Output topic: " OUTPUT_TOPIC
    read -rp "Parallelism: " PARALLELISM

    "${FLINK_HOME}"/bin/flink run -m yarn-cluster -d -c $FLINK_CLASS "$FLINK_JOB_JAR_PATH" --parallelism "$PARALLELISM" --input-topic "$INPUT_TOPIC" --output-topic "$OUTPUT_TOPIC" --bootstrap-server "$BOOTSTRAP_SERVER"
    ;;
  "2")
    FLINK_CLASS="com.paultech.Latency"
    read -rp "kafka bootstrap server: " BOOTSTRAP_SERVER
    read -rp "Input topic: " INPUT_TOPIC
    read -rp "Output topic: " OUTPUT_TOPIC
    read -rp "Parallelism: " PARALLELISM
    read -rp "Window size: " WINDOW_SIZE

    "${FLINK_HOME}"/bin/flink run -m yarn-cluster -d -c $FLINK_CLASS "$FLINK_JOB_JAR_PATH" --parallelism "$PARALLELISM" --input-topic "$INPUT_TOPIC" --output-topic "$OUTPUT_TOPIC" --bootstrap-server "$BOOTSTRAP_SERVER" --windowsize "${WINDOW_SIZE:-60}"
    ;;
  "0")
    echo "Bye"
    exit 0
    ;;
  *)
    echo "Invalid choice. Exit."
    exit -1
    ;;
  esac
}
