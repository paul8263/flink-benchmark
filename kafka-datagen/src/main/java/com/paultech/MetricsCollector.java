package com.paultech;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MetricsCollector implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsCollector.class);
    private final List<KafkaMessageSender> kafkaMessageSenderList;

    private long messagesInBytesAcc = 0;
    private long messagesCountAcc = 0;

    public MetricsCollector(List<KafkaMessageSender> kafkaMessageSenderList) {
        this.kafkaMessageSenderList = kafkaMessageSenderList;
    }

    @Override
    public void run() {
        for (KafkaMessageSender kafkaMessageSender : kafkaMessageSenderList) {
            long messagesSentTotal = kafkaMessageSender.getMessagesSentTotal().get();
            long messagesSendInBytes = kafkaMessageSender.getMessagesSentInBytes().get();
            messagesCountAcc += messagesSentTotal;
            messagesInBytesAcc += messagesSendInBytes;
        }

        LOGGER.info("Messages sent in count: {} messages", messagesCountAcc);
        LOGGER.info("Messages sent in bytes: {} bytes", messagesInBytesAcc);
    }
}
