package com.paultech;

import com.codahale.metrics.Histogram;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class AnalyzerTask implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AnalyzerTask.class);
    private KafkaConsumer<String, String> kafkaConsumer;
    private Histogram histogram;

    private volatile boolean isWorking = true;

    public AnalyzerTask(String topic, Properties properties, Histogram histogram) {
        kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList(topic));
        this.histogram = histogram;
    }

    @Override
    public void run() {

        while (isWorking) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                String value = consumerRecord.value();
                LOGGER.info("Value: {}", value);
                String[] split = value.split(" ");
                histogram.update(Long.parseLong(split[1]) - Long.parseLong(split[0]));
            }
        }
    }

    public void close() {
        isWorking = false;
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            LOGGER.error(e.toString());
        }
        if (null != kafkaConsumer) {
            LOGGER.info("Close Kafka Consumer");
            kafkaConsumer.close();
        }
    }
}
