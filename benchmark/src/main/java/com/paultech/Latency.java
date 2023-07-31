package com.paultech;

import com.paultech.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Latency {
    private static final Logger LOGGER = LoggerFactory.getLogger(Latency.class);
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    env.getConfig.setLatencyTrackingInterval(100)

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        LOGGER.info("Parameters: {}", parameterTool.toMap().toString());

        int parallelism = parameterTool.getInt("parallelism", 12);
        env.setParallelism(parallelism);
        if (parameterTool.has("bufferTimeout")) {
            env.setBufferTimeout(parameterTool.getLong("bufferTimeout"));
        }

        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(parameterTool);
        KafkaSink<String> kafkaSink = KafkaUtil.getKafkaSink(parameterTool);

        SingleOutputStreamOperator<String> dataStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");

        dataStream.flatMap((FlatMapFunction<String, String>) (value, collector) -> {
            int spaceIndex = value.indexOf(" ");
            if (spaceIndex == -1) {
                LOGGER.error("Kafka input value: {} is illegal. Ignored.", value);
                return;
            }
            String eventTime = value.substring(0, spaceIndex);
            long processingTime = System.currentTimeMillis();
            collector.collect(eventTime + " " + processingTime);
        }).returns(Types.STRING).name("latency-map")
            .sinkTo(kafkaSink)
            .name("kafka-sink");

        env.execute("Latency and Throughput Job");
    }
}
