package com.paultech;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class Demo {
    public static void main(String[] args) {
        /*KafkaProducerConfig kafkaProducerConfig = new KafkaProducerConfig();

        kafkaProducerConfig.setBootstrapServer("10.180.210.187:6667");
        Properties properties = kafkaProducerConfig.toProperties();
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        String msg = System.currentTimeMillis() + " " + UUID.randomUUID().toString();
        String key = String.valueOf(Utils.murmur2(msg.getBytes()));
        ProducerRecord<String, String> stringStringProducerRecord = new ProducerRecord<>("test", key, msg);
        kafkaProducer.send(stringStringProducerRecord);
        kafkaProducer.flush();

        kafkaProducer.close();*/

        kafkaConsumer();
    }

    public static void kafkaConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.180.210.187:6667,10.180.210.188:6667,10.180.210.189:6667");
        props.put("group.id", "group-1");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(Arrays.asList("output1"));
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, value = %s", record.offset(), record.value());
                System.out.println();
            }
        }
    }
}
