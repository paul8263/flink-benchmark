package com.paultech;

import com.paultech.payloadgenerator.PayloadGenerator;
import com.paultech.payloadgenerator.PayloadGeneratorBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class KafkaMessageSender implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMessageSender.class);
    private final KafkaProducer<String, String> kafkaProducer;
    private final String topic;
    private final long messagesPerInterval;
    private final PayloadGenerator payloadGenerator;

    private final AtomicLong messagesSentTotal = new AtomicLong();
    private final AtomicLong messagesSentInBytes = new AtomicLong();
    public KafkaMessageSender(CommandLineOpt commandLineOpt) {
        this.kafkaProducer = new KafkaProducer<>(commandLineOpt.buildKafkaProperties());
        this.topic = commandLineOpt.getTopic();
        this.messagesPerInterval = commandLineOpt.getMessagesPerInterval();
        this.payloadGenerator = PayloadGeneratorBuilder.build(commandLineOpt.getPayloadType());
    }

    public AtomicLong getMessagesSentTotal() {
        return messagesSentTotal;
    }

    public AtomicLong getMessagesSentInBytes() {
        return messagesSentInBytes;
    }

    @Override
    public void run() {
        messagesSentInBytes.addAndGet(sendMessage());
        messagesSentTotal.addAndGet(messagesPerInterval);
    }

    public long sendMessage() {
        int messageSent = 0;
        long messagesSentInBytes = 0;
        String msg = System.currentTimeMillis() + " " + payloadGenerator.generatePayload();
        String key = String.valueOf(Utils.murmur2(msg.getBytes()));
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Message sent: {} . Thread: {}", msg, Thread.currentThread().getName());
        }
        ProducerRecord<String, String> stringStringProducerRecord = new ProducerRecord<>(topic, key, msg);
        while (messageSent < messagesPerInterval) {
            kafkaProducer.send(stringStringProducerRecord);
            messageSent++;
            messagesSentInBytes += payloadGenerator.getPayloadSize();
        }
        return messagesSentInBytes;
    }

    public void close() {
        if (null != kafkaProducer) {
            kafkaProducer.close();
        }
    }
}
