package com.paultech;

import java.util.Properties;

public class KafkaConsumerConfig {
    private String bootstrapServer;
    private String groupId;
    private String enableAutoCommit = "true";
    private String autoCommitIntervalMs = "1000";
    private String keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
    private String valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";

    public KafkaConsumerConfig() {}

    public KafkaConsumerConfig(LatencyCommandOpt latencyCommandOpt) {
        this.bootstrapServer = latencyCommandOpt.getBootstrapServers();
        this.groupId = latencyCommandOpt.getGroupId();
    }

    public Properties toProperties() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServer);
        props.setProperty("group.id", groupId);
        props.setProperty("enable.auto.commit", enableAutoCommit);
        props.setProperty("auto.commit.interval.ms", autoCommitIntervalMs);
        props.setProperty("key.deserializer", keyDeserializer);
        props.setProperty("value.deserializer", valueDeserializer);
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("session.timeout.ms", "30000");
        return props;
    }
}
