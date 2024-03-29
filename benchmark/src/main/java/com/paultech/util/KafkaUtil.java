package com.paultech.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaUtil.class);

    public static KafkaSource<String> getKafkaSource(ParameterTool parameterTool) {
        String bootstrapServer = parameterTool.get("bootstrap-server", "localhost:9092");
        String sourceKafkaTopic = parameterTool.get("input-topic", "input-topic");
        String consumerGroup = parameterTool.get("consumer-group", "flink-bench");
        String offset = parameterTool.get("offset", "latest").toLowerCase();

        LOGGER.info("Building Kafka source");
        LOGGER.info("Bootstrap server: {}", bootstrapServer);
        LOGGER.info("Source topic: {}", sourceKafkaTopic);
        LOGGER.info("Consumer group: {}", consumerGroup);
        LOGGER.info("Offset: {}", offset);

        KafkaSourceBuilder<String> builder = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServer)
            .setTopics(sourceKafkaTopic)
            .setGroupId(consumerGroup)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema());

        switch (offset) {
            case "earliest":
                builder.setStartingOffsets(OffsetsInitializer.earliest());
                break;
            case "latest":
            default:
                builder.setStartingOffsets(OffsetsInitializer.latest());
                break;
        }
        return builder.build();
    }

    public static KafkaSink<String> getKafkaSink(ParameterTool parameterTool) {
        String bootstrapServer = parameterTool.get("bootstrap-server", "localhost:9092");
        String outputTopic = parameterTool.get("output-topic", "output-topic");

        LOGGER.info("Building Kafka sink");
        LOGGER.info("Bootstrap server: {}", bootstrapServer);
        LOGGER.info("Sink topic: {}", outputTopic);

        return KafkaSink.<String>builder()
            .setBootstrapServers(bootstrapServer)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(outputTopic)
                .setValueSerializationSchema(new SimpleStringSchema())
                .build()
            )
            .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
    }
}
