package com.paultech;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MetricsCollector implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsCollector.class);
    private final List<KafkaMessageSender> kafkaMessageSenderList;

    private static long messagesCountAcc = 0L;
    private static long messagesInBytesAcc = 0L;

    public MetricsCollector(List<KafkaMessageSender> kafkaMessageSenderList) {
        this.kafkaMessageSenderList = kafkaMessageSenderList;
    }

    public static long getMessagesCountAcc() {
        return messagesCountAcc;
    }

    public static long getMessagesInBytesAcc() {
        return messagesInBytesAcc;
    }

    @Override
    public void run() {
        long messagesInBytesAcc = 0;
        long messagesCountAcc = 0;
        for (KafkaMessageSender kafkaMessageSender : kafkaMessageSenderList) {
            long messagesSentTotal = kafkaMessageSender.getMessagesSentTotal().get();
            long messagesSendInBytes = kafkaMessageSender.getMessagesSentInBytes().get();
            messagesCountAcc += messagesSentTotal;
            messagesInBytesAcc += messagesSendInBytes;
        }

        LOGGER.info("Messages sent in count: {} messages", messagesCountAcc);
        LOGGER.info("Messages sent in bytes: {} bytes", messagesInBytesAcc);

        MetricsCollector.messagesCountAcc = messagesCountAcc;
        MetricsCollector.messagesInBytesAcc = messagesInBytesAcc;
    }
}
