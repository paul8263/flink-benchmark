# Flink benchmark

This project is targeted to test Flink data throughput and latency with Kafka data source.

# Author

Paul Zhang

# Environment Requirement

* Flink 1.13.2
* Hadoop Yarn cluster
* Kafka 1.1.1

If you have deployed Flink/Kafka with different version, please update Flink/Kafka version properties in `pom.xml` before compilation.

# Compilation

```shell
mvn clean package
```

The binary distribution locates in `benchmark-dist/target/dist/flink-benchmark`.

# Run benchmarks

## Latency and Throughput

Note: The number of partitions for both input/output Kafka topic should be equal to the number of threads for Kafka datagen and parallelisms for Flink benchmark job.

Explanations of parameters are listed in [Command options](#command-options)

### 1. Create Kafka topic

If the input/output topic already exist, please delete them first.

```shell
./kafka-topics.sh --zookeeper manager.bigdata:2181,master.bigdata:2181,worker.bigdata:2181 --delete --topic input
./kafka-topics.sh --zookeeper manager.bigdata:2181,master.bigdata:2181,worker.bigdata:2181 --delete --topic output
```

Create input/output topic with designated number of partitions.
```shell
# Create topic with designated partitions
./kafka-topics.sh --create --zookeeper manager.bigdata:2181,master.bigdata:2181,worker.bigdata:2181 --replication-factor 1 --partitions 4 --topic output
./kafka-topics.sh --create --zookeeper manager.bigdata:2181,master.bigdata:2181,worker.bigdata:2181 --replication-factor 1 --partitions 4 --topic input
```

### 2. Start Flink job

```shell
# Latency and Throughput benchmark
./bin/flink run -m yarn-cluster -c com.paultech.Latency /path/to/benchmark/benchmark-1.0.jar --parallelism 4 --output-topic output --input-topic input --bootstrap-server kafka01:6667,kafka02:6667,kafka03:6667
```

### 3. Start Kafka datagen

```shell
java -jar kafka-datasource-1.0.jar -t input -b kafka01:6667,kafka02:6667,kafka03:6667 -i 10 -c 100 -n 4
```

### 4. Get result

Your need to run result analyzer to calculate the histogram of latency.

```shell script
java -jar kafka-result-analyzer-1.0.jar -b kafka01:6667,kafka02:6667,kafka03:6667 -t output
```

> Flink might need some time to consume all pending records, so you might need run result analyzer several times.

## Window throughput benchmark

### 1. Start Kafka datagen

```shell
java -jar kafka-datasource-1.0.jar -t test_topic -b kafka01:6667,kafka02:6667,kafka03:6667 -a 0 -i 10 -n 4 -p uuid
```

### 2. Start Flink Job

```shell
# Window Throughput benchmark
./bin/flink run -m yarn-cluster -c com.paultech.WindowThroughput /path/to/benchmark/benchmark-1.0.jar --parallelism 4 --output-topic output --input-topic input --bootstrap-server kafka01:6667,kafka02:6667,kafka03:6667
```

Data will be collected in 1-minute-window. 

### 3. Get result

Use the following command to retrieve the result from output topic:

```shell
./kafka-console-consumer.sh --bootstrap-server kafka01:6667,kafka02:6667,kafka03:6667 --topic output
```

The output is how many records in a 1-minute-long window that Flink is able to process.

# Command options

## Flink job command options

* --parallelism: Parallelism for Flink Stream Execution Environment
* --bufferTimeout: Flink buffer timeout
* --input-topic: Kafka topic where Flink reads data
* --output-topic: Kafka topic where Flink writes data
* --bootstrap-server: Addresses and ports for kafka brokers
* --consumer-group: Consumer group. Default is "flink-bench"
* --startFromEarliest: Consume kafka topic from the earliest offset
* --input: Input file path
* --output: output file path

Examples:

```shell
# Run Throughput
./bin/flink run -m 10.180.210.187:8081 -c com.paultech.WindowThroughput /root/zy/benchmark/benchmark-1.0.jar --parallelism 12 --output-topic output --input-topic input --bootstrap-server 10.180.210.187:6667,10.180.210.188:6667,10.180.210.189:6667

# Run Latency
./bin/flink run -m 10.180.210.187:8081 -c com.paultech.Latency /root/zy/benchmark/benchmark-1.0.jar --parallelism 12 --output-topic output --input-topic input --bootstrap-server 10.180.210.187:6667,10.180.210.188:6667,10.180.210.189:6667

# Run Word Count
./bin/flink run -m 10.180.210.187:8081 -c com.paultech.WordCount /root/zy/benchmark/benchmark-1.0.jar --parallelism 12 --output hdfs:///output.txt --input hdfs:///input.txt
```

## Kafka datagen command options

* -b: Kafka bootstrap server
* -t: Output Kafka topic
* -a: Acks
* -n: Number of threads
* -i: Message send interval
* -c: Messages send per interval
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

## Start benchmark via step-by-step guide

1. Enter `flink-benchmark/benchmark-dist/target/dist/benchmark-dist/bin`.
2. Update `benchmark-env.sh` by setting `FLINK_HOME` and `JAVA_HOME`(optional).
3. Execute `run.sh` and then follow the instruction.
