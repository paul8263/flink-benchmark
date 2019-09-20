# Compilation

```shell script
mvn clean package
```

# How to run benchmark

## Benchmark command options

* --parallelism: Parallelism for Flink Stream Execution Environment
* --input-topic: Kafka topic where Flink reads data
* --output-topic: Kafka topic where Flink writes data
* --bootstrap-server: Addresses and ports for kafka brokers
* --input: Input file path
* --output: output file path
* --startFromEarliest: Consume kafka topic from earliest offset

### Examples
```shell script
# Run Throughput
./bin/flink run -m 10.180.210.187:8081 -c com.paultech.Throughput /root/zy/benchmark/benchmark-1.0-SNAPSHOT.jar --parallelism 12 --output-topic output --input-topic input --bootstrap-server 10.180.210.187:6667,10.180.210.188:6667,10.180.210.189:6667

# Run Latency
./bin/flink run -m 10.180.210.187:8081 -c com.paultech.Latency /root/zy/benchmark/benchmark-1.0-SNAPSHOT.jar --parallelism 12 --output-topic output --input-topic input --bootstrap-server 10.180.210.187:6667,10.180.210.188:6667,10.180.210.189:6667

# Run Word Count
./bin/flink run -m 10.180.210.187:8081 -c com.paultech.WordCount /root/zy/benchmark/benchmark-1.0-SNAPSHOT.jar --parallelism 12 --output hdfs:///output.txt --input hdfs:///input.txt

# Run Table SQL
./bin/flink run -m 10.180.210.187:8081 -c com.paultech.TableSQL /root/zy/benchmark/benchmark-1.0-SNAPSHOT.jar --parallelism 12 --output hdfs:///output.csv --input hdfs:///input.csv
```

# How to run Kafka datasource

```shell script
java -jar kafka-datasource-1.0-SNAPSHOT.jar -b 10.180.210.187:6667,10.180.210.188:6667,10.180.210.189:6667 -t input -a 0 -n 12
```

## Parameters

* -b: Kafka bootstrap server
* -t: Output Kafka topic
* -a: Acks
* -n: Number of threads

# How to run Kafka latency analyzer

```shell script
java -jar kafka-latency-analyzer-1.0-SNAPSHOT.jar -b 10.180.210.187:6667,10.180.210.188:6667,10.180.210.189:6667 -t output1 -g analyzer
```

## Parameters

* -b: Kafka bootstrap server
* -t: Output Kafka topic
* -g: Kafka consumer group
