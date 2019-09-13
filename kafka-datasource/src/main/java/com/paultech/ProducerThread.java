package com.paultech;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Utils;

import java.util.Properties;
import java.util.UUID;

public class ProducerThread implements Runnable {
    private KafkaProducer<String, String> kafkaProducer;
    private String topic;

    private volatile boolean isWorking = true;

    public ProducerThread(Properties properties, String topic) {
        this.kafkaProducer = new KafkaProducer<String, String>(properties);
        this.topic = topic;
    }

    @Override
    public void run() {
        while (isWorking) {
            String msg = System.currentTimeMillis() + " " + UUID.randomUUID().toString();
            String key = String.valueOf(Utils.murmur2(msg.getBytes()));
            ProducerRecord<String, String> stringStringProducerRecord = new ProducerRecord<>(topic, key, msg);
            kafkaProducer.send(stringStringProducerRecord);
        }
    }

    public void setWorking(boolean working) {
        isWorking = working;
    }
}
