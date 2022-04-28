# Flink benchmark

This project is targeted to test Flink data throughput and latency with Kafka data source.

# Author

Paul Zhang

# Environment Requirement

* Flink
* Hadoop Yarn cluster
* Kafka

# Compilation

```shell
mvn clean package
```

# How to run benchmark

## 1. Start Kafka datagen

```shell
java -jar kafka-datasource-1.0-SNAPSHOT.jar -t test_topic -b kafka01:6667,kafka02:6667,kafka03:6667 -a 0 -i 10 -n 1 -p uuid
```
## 2. Start Flink Job

```shell
# Benchmark Throughput
./bin/flink run -m yarn-cluster -c com.paultech.Throughput /path/to/benchmark/benchmark-1.0-SNAPSHOT.jar --parallelism 12 --output-topic output --input-topic input --bootstrap-server kafka01:6667,kafka02:6667,kafka03:6667

# Benchmark Latency
./bin/flink run -m yarn-cluster -c com.paultech.Latency /path/to/benchmark/benchmark-1.0-SNAPSHOT.jar --parallelism 12 --output-topic output --input-topic input --bootstrap-server kafka01:6667,kafka02:6667,kafka03:6667
```

## 3. Get result

### Throughput benchmark

Data will be collected in 1-minute-window. Use the following command to retrieve the output from output topic:

```shell
./kafka-console-consumer.sh --bootstrap-server kafka01:6667,kafka02:6667,kafka03:6667 --topic output
```

The output is how many records per minute the Flink is able to process.

### Latency benchmark

Your need to run latency analyzer to calculate the histogram of latency.

```shell script
java -jar kafka-latency-analyzer-1.0-SNAPSHOT.jar -b kafka01:6667,kafka02:6667,kafka03:6667 -t output
```

# Command options

## Flink job command options

* --parallelism: Parallelism for Flink Stream Execution Environment
* --input-topic: Kafka topic where Flink reads data
* --output-topic: Kafka topic where Flink writes data
* --bootstrap-server: Addresses and ports for kafka brokers
* --input: Input file path
* --output: output file path
* --startFromEarliest: Consume kafka topic from earliest offset

Examples:

```shell
# Run Throughput
./bin/flink run -m 10.180.210.187:8081 -c com.paultech.Throughput /root/zy/benchmark/benchmark-1.0-SNAPSHOT.jar --parallelism 12 --output-topic output --input-topic input --bootstrap-server 10.180.210.187:6667,10.180.210.188:6667,10.180.210.189:6667

# Run Latency
./bin/flink run -m 10.180.210.187:8081 -c com.paultech.Latency /root/zy/benchmark/benchmark-1.0-SNAPSHOT.jar --parallelism 12 --output-topic output --input-topic input --bootstrap-server 10.180.210.187:6667,10.180.210.188:6667,10.180.210.189:6667

# Run Word Count
./bin/flink run -m 10.180.210.187:8081 -c com.paultech.WordCount /root/zy/benchmark/benchmark-1.0-SNAPSHOT.jar --parallelism 12 --output hdfs:///output.txt --input hdfs:///input.txt
```

## Kafka datagen command options

* -b: Kafka bootstrap server
* -t: Output Kafka topic
* -a: Acks
* -n: Number of threads
* -i: Message send interval
* -p: Kafka data payload type. Can be uuid or 1kb
* -h: Get help message

Examples:

```shell
java -jar kafka-datasource-1.0-SNAPSHOT.jar -b 10.180.210.187:6667,10.180.210.188:6667,10.180.210.189:6667 -t input -a 0 -n 12
```

## Kafka latency analyzer command options


* -b: Kafka bootstrap server
* -t: Output Kafka topic
* -g: Kafka consumer group

Examples:

```shell script
java -jar kafka-latency-analyzer-1.0-SNAPSHOT.jar -b 10.180.210.187:6667,10.180.210.188:6667,10.180.210.189:6667 -t output1 -g analyzer
```

# Appendix

## Start benchmark via step-by-stp guide

1. Enter `flink-benchmark/benchmark-dist/target/dist/benchmark-dist/bin`.
2. Update `benchmark-env.sh` by setting `FLINK_HOME` and `JAVA_HOME`(optional).
3. Execute `run.sh` and then follow the instruction.
