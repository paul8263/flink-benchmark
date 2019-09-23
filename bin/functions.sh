#!/usr/bin/env bash

FLINK_JOB_JAR_NAME="benchmark-1.0-SNAPSHOT.jar"
KAFKA_DATASOURCE_JAR_NAME="kafka-datasource-1.0-SNAPSHOT.jar"
KAFKA_LATENCT_ANALYZER_JAR_NAME="kafka-latenct-analyzer-1.0-SNAPSHOT.jar"

CURRENT_DIR=$(cd `dirname $0`; pwd)

FLINK_JOB_JAR_PATH=$CURRENT_DIR/../benchmark/target/$FLINK_JOB_JAR_NAME
KAFKA_DATASOURCE_JAR_PATH=$CURRENT_DIR/../kafka-datasource/$KAFKA_DATASOURCE_JAR_NAME
KAFKA_LATENCT_ANALYZER_JAR_PATH=$CURRENT_DIR/../kafka-latency-analyzer/$KAFKA_LATENCT_ANALYZER_JAR_NAME

welcome() {
  echo "=============================================="
  echo "====Flink benchmark project v1.0 snapshot====="
  echo "============Author: Paul Zhang================"
  echo -e "\n"
}

display_path() {
  echo $FLINK_JOB_JAR_PATH
  echo $KAFKA_DATASOURCE_JAR_PATH
  echo $KAFKA_LATENCT_ANALYZER_JAR_PATH
}

command_exists() {
  type $1 > /dev/null
}

set_java_command() {
  command_exists java
  if [[ $? -eq 0 ]]; then
    JAVA_COMMAND="java"
  else
    if [[ -e $JAVA_HOME ]]; then
      JAVA_COMMAND=$JAVA_HOME
    fi
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

run_kafka_datasource() {
  echo "Run Kafka datasource"
  read -p "Kafka bootstrap server: " BOOTSTRAP_SERVER
  read -p "Input topic: " TOPIC
  read -p "Number of threads: " THREADS

  $JAVA_COMMAND -jar $KAFKA_DATASOURCE_JAR_PATH -b $BOOTSTRAP_SERVER -n $THREADS -a 0 -t $TOPIC
}


run_kafka_latency_analyzer() {
  echo "Run Kafka latency analyzer"
  read -p "Kafka bootstrap server: " BOOTSTRAP_SERVER
  read -p "Output topic: " TOPIC
  read -p "Consumer group: " GROUP

  $JAVA_COMMAND -jar $KAFKA_LATENCT_ANALYZER_JAR_PATH -b $BOOTSTRAP_SERVER -t $TOPIC -g $GROUP
}

run_benchmark() {
  echo "Run benchmark"
  echo "Type of benchmark:"
  echo "1) Throughput"
  echo "2) Latency"
  echo "3) Word Count"
  echo "4) Table SQL"

  read -p "Your choice(0 to exit): " CHOICE
  case "$CHOICE" in
  "1")
  FLINK_CLASS="com.paultech.Throughput"
  read -p "Flink Job Manager(IP:Port): " FLINK_JOB_Manager
  read -p "kafka bootstrap server: " BOOTSTRAP_SERVER
  read -p "Input topic: " INPUT_TOPIC
  read -p "Output topic: " OUTPUT_TOPIC
  read -p "Parallelism: " PARALLELISM

  $FLINK_HOME -m $FLINK_JOB_Manager -d -c $FLINK_CLASS $FLINK_JOB_JAR_NAME --parallelism $PARALLELISM --input-topic $INPUT_TOPIC --output-topic $OUTPUT_TOPIC --bootstrap-server $BOOTSTRAP_SERVER
  ;;
  "2")
  FLINK_CLASS="com.paultech.Latency"
  read -p "Flink Job Manager(IP:Port): " FLINK_JOB_Manager
  read -p "kafka bootstrap server: " BOOTSTRAP_SERVER
  read -p "Input topic: " INPUT_TOPIC
  read -p "Output topic: " OUTPUT_TOPIC
  read -p "Parallelism: " PARALLELISM

  $FLINK_HOME -m $FLINK_JOB_Manager -d -c $FLINK_CLASS $FLINK_JOB_JAR_NAME --parallelism $PARALLELISM --input-topic $INPUT_TOPIC --output-topic $OUTPUT_TOPIC --bootstrap-server $BOOTSTRAP_SERVER
  ;;
  "3")
  FLINK_CLASS="com.paultech.WordCount"
  read -p "Flink Job Manager(IP:Port): " FLINK_JOB_Manager
  read -p "Input file: " INPUT_FILE
  read -p "Output file: " OUTPUT_FILE
  read -p "Parallelism: " PARALLELISM

  $FLINK_HOME -m $FLINK_JOB_Manager -d -c $FLINK_CLASS $FLINK_JOB_JAR_NAME --parallelism $PARALLELISM --input $INPUT_FILE --output $OUTPUT_FILE
  ;;
  "4")
  FLINK_CLASS="com.paultech.TableSQL"
  read -p "Flink Job Manager(IP:Port): " FLINK_JOB_Manager
  read -p "Input file: " INPUT_FILE
  read -p "Output file: " OUTPUT_FILE
  read -p "Parallelism: " PARALLELISM

  $FLINK_HOME -m $FLINK_JOB_Manager -d -c $FLINK_CLASS $FLINK_JOB_JAR_NAME --parallelism $PARALLELISM --input $INPUT_FILE --output $OUTPUT_FILE
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
