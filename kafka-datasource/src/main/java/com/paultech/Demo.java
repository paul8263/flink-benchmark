package com.paultech;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Utils;

import java.util.Properties;
import java.util.UUID;

public class Demo {
    public static void main(String[] args) {
        KafkaConfig kafkaConfig = new KafkaConfig();

        kafkaConfig.setBootstrapServer("10.180.210.187:6667");
        Properties properties = kafkaConfig.toProperties();
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        String msg = System.currentTimeMillis() + " " + UUID.randomUUID().toString();
        String key = String.valueOf(Utils.murmur2(msg.getBytes()));
        ProducerRecord<String, String> stringStringProducerRecord = new ProducerRecord<>("test", key, msg);
        kafkaProducer.send(stringStringProducerRecord);
        kafkaProducer.flush();

        kafkaProducer.close();
    }
}
