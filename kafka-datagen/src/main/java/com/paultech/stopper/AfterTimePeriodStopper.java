package com.paultech.stopper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class AfterTimePeriodStopper implements Stopper {
    public static final Logger LOGGER = LoggerFactory.getLogger(AfterTimePeriodStopper.class);

    public static final long DEFAULT_RUNNING_DURATION_SEC = 60L;

    private final long runningDurationSec;
    private final ExecutorService messageSenderExecutorService;
    private final ExecutorService metricsExecutorService;

    public AfterTimePeriodStopper(long runningDurationMs, ExecutorService messageSenderExecutorService, ExecutorService metricsExecutorService) {
        this.runningDurationSec = runningDurationMs;
        this.messageSenderExecutorService = messageSenderExecutorService;
        this.metricsExecutorService = metricsExecutorService;
    }

    @Override
    public void stop() {
        try {
            long sleepingTimeInMs = runningDurationSec <= 0 ? DEFAULT_RUNNING_DURATION_SEC * 1000 : runningDurationSec * 1000;
            LOGGER.info("Data Generator will be running for {} secs", sleepingTimeInMs);
            Thread.sleep(sleepingTimeInMs);
            LOGGER.info("Data Generator has been running for {} secs. Now stop datagen.", sleepingTimeInMs);
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
