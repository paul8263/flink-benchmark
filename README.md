# 运行方法

```shell script
./bin/flink run -m 10.180.210.187:8081 -c com.paultech.Throughput /root/zy/benchmark/benchmark-1.0-SNAPSHOT.jar --parallelism 12 --output-topic output --input-topic test --bootstrap-server 10.180.210.187:6667,10.180.210.188:6667,10.180.210.189:6667
```