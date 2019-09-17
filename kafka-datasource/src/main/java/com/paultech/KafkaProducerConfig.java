package com.paultech;

import java.util.Properties;

public class KafkaProducerConfig {
    private String bootstrapServer;
    private String topic;
    private String acks = "all";
    private int lingerms = 1;
    private int retries = 0;
    private int bufferMemory = 33554432;
    private int batchSize = 16384;
    private String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";
    private String valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";

    public String getBootstrapServer() {
        return bootstrapServer;
    }

    public void setBootstrapServer(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getAcks() {
        return acks;
    }

    public void setAcks(String acks) {
        this.acks = acks;
    }

    public int getLingerms() {
        return lingerms;
    }

    public void setLingerms(int lingerms) {
        this.lingerms = lingerms;
    }

    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public int getBufferMemory() {
        return bufferMemory;
    }

    public void setBufferMemory(int bufferMemory) {
        this.bufferMemory = bufferMemory;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getKeySerializer() {
        return keySerializer;
    }

    public void setKeySerializer(String keySerializer) {
        this.keySerializer = keySerializer;
    }

    public String getValueSerializer() {
        return valueSerializer;
    }

    public void setValueSerializer(String valueSerializer) {
        this.valueSerializer = valueSerializer;
    }

    public Properties toProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServer);
        properties.put("acks", acks);
        properties.put("retries", retries);
        properties.put("batch.size", batchSize);
        properties.put("linger.ms", lingerms);
        properties.put("buffer.memory", bufferMemory);
        properties.put("key.serializer", keySerializer);
        properties.put("value.serializer", valueSerializer);
        return properties;
    }
}
