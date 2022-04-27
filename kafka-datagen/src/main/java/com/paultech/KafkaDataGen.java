package com.paultech;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Generate Kafka test data
 */
public class KafkaDataGen {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDataGen.class);

    private static final int RUNNING_DURATION_MINUTE = 10;

    public static void main(String[] args) {
        CommandLineOpt commandLineOpt = CommandLineOpt.parseCommandLine(args);
        LOGGER.info(commandLineOpt.toString());

        Properties kafkaProperties = commandLineOpt.buildKafkaProperties();
        int numOfThreads = commandLineOpt.getNumberOfThreads();

        List<ProducerThread> producerThreadList = generateProducerThread(commandLineOpt);
        ExecutorService executorService = Executors.newFixedThreadPool(numOfThreads);
        for (ProducerThread producerThread : producerThreadList) {
            executorService.submit(producerThread);
        }

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(RUNNING_DURATION_MINUTE, TimeUnit.MINUTES)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOGGER.error(e.toString());
            executorService.shutdownNow();
        }
    }

    private static List<ProducerThread> generateProducerThread(CommandLineOpt commandLineOpt) {
        int numberOfThreads = commandLineOpt.getNumberOfThreads();
        List<ProducerThread> producerThreadList = new ArrayList<>(numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            producerThreadList.add(new ProducerThread(commandLineOpt));
        }
        return producerThreadList;
    }
}
