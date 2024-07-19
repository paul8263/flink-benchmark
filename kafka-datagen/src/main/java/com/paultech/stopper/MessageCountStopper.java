package com.paultech.stopper;

import com.paultech.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class MessageCountStopper implements Stopper {
    public static final Logger LOGGER = LoggerFactory.getLogger(MessageCountStopper.class);

    private final long messageCount;
    private final ExecutorService messageSenderExecutorService;
    private final ExecutorService metricsExecutorService;

    public MessageCountStopper(long messageCount, ExecutorService messageSenderExecutorService, ExecutorService metricsExecutorService) {
        this.messageCount = messageCount;
        this.messageSenderExecutorService = messageSenderExecutorService;
        this.metricsExecutorService = metricsExecutorService;
    }

    @Override
    public void stop() {
        try {
            long messagesCountAcc = MetricsCollector.getMessagesCountAcc();
            while (messageCount < messagesCountAcc) {
                LOGGER.info("Number of messages has been sent: {}. Target message count: {}.", messagesCountAcc, messageCount);
                Thread.sleep(MetricsCollector.METRICS_COLLECTOR_INTERVAL_SEC);
                messagesCountAcc = MetricsCollector.getMessagesCountAcc();
            }
            LOGGER.info("Number of messages sent has exceeded target message count. Stopping datagen");

            messageSenderExecutorService.shutdown();
            metricsExecutorService.shutdown();
            if (!messageSenderExecutorService.awaitTermination(AWAIT_TIME_OUT, TimeUnit.MILLISECONDS)) {
                messageSenderExecutorService.shutdownNow();
                metricsExecutorService.shutdownNow();
            }

        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
        }
    }
}
