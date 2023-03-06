package com.paultech;

import com.codahale.metrics.Histogram;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class AnalyzerTask implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AnalyzerTask.class);
    private final KafkaConsumer<String, String> kafkaConsumer;
    private final Histogram histogram;
    private final AnalyzerResult analyzerResult = new AnalyzerResult();

    private volatile boolean isWorking = true;

    public AnalyzerTask(ResultCommandOpt resultCommandOpt, Histogram histogram) {
        kafkaConsumer = new KafkaConsumer<>(resultCommandOpt.buildKafkaProperties());
        kafkaConsumer.subscribe(Collections.singletonList(resultCommandOpt.getTopic()));
        this.histogram = histogram;
    }

    public AnalyzerResult getAnalyzerResult() {
        return analyzerResult;
    }

    @Override
    public void run() {
        while (isWorking) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                String value = consumerRecord.value();
                LOGGER.info("Value: {}", value);
                String[] split = value.split(" ");
                long endTime = Long.parseLong(split[1]);
                long startTime = Long.parseLong(split[0]);
                histogram.update(Math.max(0, endTime - startTime));
                analyzerResult.update(startTime, endTime);
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
