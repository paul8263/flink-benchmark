package com.paultech.stopper;

import com.paultech.CommandLineOpt;

import java.util.concurrent.ScheduledExecutorService;

public class StopperBuilder {
    public static Stopper build(CommandLineOpt commandLineOpt, ScheduledExecutorService messageSenderExecutorService, ScheduledExecutorService metricsExecutorService) {
        // If stopAfterMessageCount is set, use MessageCountStopper, otherwise use AfterTimePeriodStopper
        if (commandLineOpt.getStopAfterMessageCount() != -1) {
            return new MessageCountStopper(commandLineOpt.getStopAfterMessageCount(), messageSenderExecutorService, metricsExecutorService);
        }
        return new AfterTimePeriodStopper(commandLineOpt.getStopAfterTimeSec(), messageSenderExecutorService, metricsExecutorService);
    }
}
