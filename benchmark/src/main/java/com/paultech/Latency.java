package com.paultech;

import com.paultech.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Latency {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    env.getConfig.setLatencyTrackingInterval(100)

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        int parallelism = parameterTool.getInt("parallelism", 12);
        env.setParallelism(parallelism);
        if (parameterTool.has("bufferTimeout")) {
            env.setBufferTimeout(parameterTool.getLong("bufferTimeout"));
        }

        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(parameterTool);
        KafkaSink<String> kafkaSink = KafkaUtil.getKafkaSink(parameterTool);

        SingleOutputStreamOperator<String> dataStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");

        dataStream.map((MapFunction<String, String>) value -> {
                int spaceIndex = value.indexOf(" ");
                String eventTime = value.substring(0, spaceIndex);
                long processingTime = System.currentTimeMillis();
                return eventTime + " " + processingTime;
            }).returns(Types.STRING).name("latency-map")
            .sinkTo(kafkaSink)
            .name("kafka-sink");

        env.execute("Latency and Throughput Job");
    }
}
