package com.paultech;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KafkaDataSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDataSource.class);

    public static int numOfThreads = 4;
    public static void main(String[] args) {
        KafkaProducerConfig kafkaProducerConfig = new KafkaProducerConfig();

        CommandLineOpt commandLineOpt = CommandLineOpt.parseCommandLine(args);

        LOGGER.info(commandLineOpt.toString());

        numOfThreads = commandLineOpt.getNumberOfThreads();

        kafkaProducerConfig.setBootstrapServer(commandLineOpt.getBootstrapServers());
        kafkaProducerConfig.setTopic(commandLineOpt.getTopic());
        kafkaProducerConfig.setAcks(commandLineOpt.getAcks());
        Properties properties = kafkaProducerConfig.toProperties();


        List<ProducerThread> producerThreadList = generateProducerThread(properties, numOfThreads, kafkaProducerConfig.getTopic());
        ExecutorService executorService = Executors.newFixedThreadPool(numOfThreads);
        for (ProducerThread producerThread : producerThreadList) {
            executorService.submit(producerThread);
        }

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(30, TimeUnit.MINUTES)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOGGER.error(e.toString());
            executorService.shutdownNow();
        }

    }

    private static List<ProducerThread> generateProducerThread(Properties properties, int numOfThreads, String topic) {
        List<ProducerThread> producerThreadList = new ArrayList<>(numOfThreads);
        for (int i = 0; i < KafkaDataSource.numOfThreads; i++) {
            producerThreadList.add(new ProducerThread(properties, topic));
        }
        return producerThreadList;
    }
}
