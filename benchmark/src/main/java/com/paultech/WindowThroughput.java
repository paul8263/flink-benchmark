package com.paultech;

import com.paultech.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WindowThroughput {

    private static final Logger LOGGER = LoggerFactory.getLogger(WindowThroughput.class);
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        LOGGER.info("Parameters: {}", parameterTool.toMap().toString());

        int parallelism = parameterTool.getInt("parallelism", 12);
        env.setParallelism(parallelism);
        if (parameterTool.has("bufferTimeout")) {
            env.setBufferTimeout(parameterTool.getLong("bufferTimeout"));
        }

        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(parameterTool);
        KafkaSink<String> kafkaSink = KafkaUtil.getKafkaSink(parameterTool);

        SingleOutputStreamOperator<String> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");

        stream
            .windowAll(TumblingProcessingTimeWindows.of(Time.minutes(1)))
            .process(new ProcessAllWindowFunction<String, String, TimeWindow>() {
                @Override
                public void process(ProcessAllWindowFunction<String, String, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                    String elementSize = String.valueOf(Iterables.size(elements));
                    LOGGER.info("Element size: {}", elementSize);
                    out.collect(elementSize);
                }
            })
            .returns(Types.STRING).name("window-process")
            .sinkTo(kafkaSink)
            .name("kafka-sink");

        env.execute("Window Throughput Job");
    }
}
