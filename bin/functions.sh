#!/usr/bin/env bash

FLINK_JOB_JAR_NAME="benchmark-*.jar"
KAFKA_DATAGEN_JAR_NAME="kafka-datagen-*.jar"
KAFKA_LATENCY_ANALYZER_JAR_NAME="kafka-latency-analyzer-*.jar"

CURRENT_DIR=$(
  cd $(dirname $0)
  pwd
)

FLINK_JOB_JAR_PATH=`readlink -f ../$FLINK_JOB_JAR_NAME`
KAFKA_DATAGEN_JAR_PATH=`readlink -f ../$KAFKA_DATAGEN_JAR_NAME`
KAFKA_LATENCY_ANALYZER_JAR_PATH=`readlink -f ../$KAFKA_LATENCY_ANALYZER_JAR_NAME`

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
  echo $FLINK_JOB_JAR_PATH
  echo $KAFKA_DATAGEN_JAR_PATH
  echo $KAFKA_LATENCY_ANALYZER_JAR_PATH
}

command_exists() {
  type $1 >/dev/null
}

set_java_command() {
  command_exists java
  if [[ $? -eq 0 ]]; then
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

  check_jar_file_exists $KAFKA_DATAGEN_JAR_PATH

  read -p "Kafka bootstrap server: " BOOTSTRAP_SERVER
  read -p "Input topic: " TOPIC
  read -p "Acks: " ACKS
  read -p "Number of threads: " THREADS
  read -p "Message send interval: " INTERVAL
  read -p "Payload type(uuid/1kb): " PAYLOAD_TYPE

  $JAVA_COMMAND -jar $KAFKA_DATAGEN_JAR_PATH -b $BOOTSTRAP_SERVER -n $THREADS -a $ACKS -t $TOPIC -i $INTERVAL -p $PAYLOAD_TYPE
}

run_kafka_latency_analyzer() {
  echo "Run Kafka latency analyzer"

  check_jar_file_exists $KAFKA_LATENCY_ANALYZER_JAR_PATH

  read -p "Kafka bootstrap server: " BOOTSTRAP_SERVER
  read -p "Output topic: " TOPIC

  $JAVA_COMMAND -jar $KAFKA_LATENCY_ANALYZER_JAR_PATH -b $BOOTSTRAP_SERVER -t $TOPIC
}

run_benchmark() {
  echo "Run benchmark"

  check_jar_file_exists $FLINK_JOB_JAR_PATH

  echo "Type of benchmark:"
  echo "1) Throughput"
  echo "2) Latency"

  read -p "Your choice(0 to exit): " CHOICE
  case "$CHOICE" in
  "1")
    FLINK_CLASS="com.paultech.Throughput"
    read -p "kafka bootstrap server: " BOOTSTRAP_SERVER
    read -p "Input topic: " INPUT_TOPIC
    read -p "Output topic: " OUTPUT_TOPIC
    read -p "Parallelism: " PARALLELISM

    export HADOOP_CLASSPATH=`hadoop classpath`
    ${FLINK_HOME}/bin/flink run -m yarn-cluster -d -c $FLINK_CLASS $FLINK_JOB_JAR_PATH --parallelism $PARALLELISM --input-topic $INPUT_TOPIC --output-topic $OUTPUT_TOPIC --bootstrap-server $BOOTSTRAP_SERVER
    ;;
  "2")
    FLINK_CLASS="com.paultech.Latency"
    read -p "kafka bootstrap server: " BOOTSTRAP_SERVER
    read -p "Input topic: " INPUT_TOPIC
    read -p "Output topic: " OUTPUT_TOPIC
    read -p "Parallelism: " PARALLELISM

    export HADOOP_CLASSPATH=`hadoop classpath`
    ${FLINK_HOME}/bin/flink run -m yarn-cluster -d -c $FLINK_CLASS $FLINK_JOB_JAR_PATH --parallelism $PARALLELISM --input-topic $INPUT_TOPIC --output-topic $OUTPUT_TOPIC --bootstrap-server $BOOTSTRAP_SERVER
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
