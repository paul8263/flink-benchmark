package com.paultech;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

public class ProducerThread implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerThread.class);
    private final KafkaProducer<String, String> kafkaProducer;
    private final String topic;
    private final long interval;
    private PayloadType payloadType = PayloadType.UUID;
    private volatile boolean isWorking = true;

    public ProducerThread(CommandLineOpt commandLineOpt) {
        this.kafkaProducer = new KafkaProducer<String, String>(commandLineOpt.buildKafkaProperties());
        this.topic = commandLineOpt.getTopic();
        this.interval = commandLineOpt.getMessageSendInterval();
        Optional.ofNullable(PayloadType.of(commandLineOpt.getPayloadType())).ifPresent((payloadType) -> {
            this.payloadType = payloadType;
        });
    }

    @Override
    public void run() {
        while (isWorking) {
            String msg = System.currentTimeMillis() + " " + generatePayload();
            String key = String.valueOf(Utils.murmur2(msg.getBytes()));
            ProducerRecord<String, String> stringStringProducerRecord = new ProducerRecord<>(topic, key, msg);
            kafkaProducer.send(stringStringProducerRecord);
            if (interval > 0) {
                try {
                    Thread.sleep(this.interval);
                } catch (InterruptedException e) {
                    LOGGER.error(e.toString());
                }
            }
        }
    }

    private String generatePayload() {
        if (payloadType == PayloadType.ONE_KB) {
            return "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Molestie at elementum eu facilisis sed odio morbi quis. Purus semper eget duis at tellus. Magna sit amet purus gravida quis blandit turpis cursus in. Diam in arcu cursus euismod quis viverra. Venenatis urna cursus eget nunc scelerisque viverra mauris in. Purus gravida quis blandit turpis cursus. Consectetur adipiscing elit duis tristique. Duis at consectetur lorem donec massa sapien faucibus. Eu turpis egestas pretium aenean pharetra magna ac placerat. Sit amet consectetur adipiscing elit duis tristique sollicitudin nibh sit. Egestas sed tempus urna et pharetra pharetra massa massa. Gravida dictum fusce ut placerat orci nulla pellentesque dignissim enim. Viverra mauris in aliquam sem fringilla ut. Risus nec feugiat in fermentum posuere urna nec. Urna duis convallis convallis tellus id interdum velit laoreet. Amet consectetur adipiscing elit duis tristique sollicitudin. Sollicitudin nibh sit amet commodo. ";
        }
        return UUID.randomUUID().toString();
    }

    public void setWorking(boolean working) {
        isWorking = working;
    }
}