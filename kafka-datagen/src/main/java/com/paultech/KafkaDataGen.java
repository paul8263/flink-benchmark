package com.paultech;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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

        createTopic(commandLineOpt);

        List<ProducerThread> producerThreadList = generateProducerThread(commandLineOpt);
        ExecutorService executorService = Executors.newFixedThreadPool(commandLineOpt.getNumberOfThreads());
        for (ProducerThread producerThread : producerThreadList) {
            executorService.submit(producerThread);
        }

        executorService.shutdown();
        logInfo(commandLineOpt);
        try {
            if (!executorService.awaitTermination(RUNNING_DURATION_MINUTE, TimeUnit.MINUTES)) {
                closeProducerThread(producerThreadList);
                Thread.sleep(1500);
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOGGER.error(e.toString());
            executorService.shutdownNow();
        }
        LOGGER.info("Data Generator exited");
    }
    private static void createTopic(CommandLineOpt commandLineOpt) {
        Properties kafkaProperties = commandLineOpt.buildKafkaProperties();
        String topic = commandLineOpt.getTopic();
        try (AdminClient adminClient = AdminClient.create(kafkaProperties)) {
            Map<String, TopicDescription> topicDescriptionMap = adminClient.describeTopics(Collections.singletonList(topic)).all().get();
            if (topicDescriptionMap.containsKey(topic)) {
                LOGGER.info("Topic \"{}\" already exists. Delete it first.", topic);
                adminClient.deleteTopics(Collections.singletonList(topic)).all().get();
            }
            // Make sure the topic has been deleted
            Thread.sleep(1000);
            LOGGER.info("Create topic: {} with numOfPartitions: {}", topic, commandLineOpt.getNumberOfThreads());
            NewTopic newTopic = new NewTopic(topic, commandLineOpt.getNumberOfThreads(), (short) 1);
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            LOGGER.error("Create topic error. Topic name: {}", topic);
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

    private static void closeProducerThread(List<ProducerThread> producerThreadList) {
        for (ProducerThread producerThread : producerThreadList) {
            producerThread.close();
        }
    }

    private static void logInfo(CommandLineOpt commandLineOpt) {
        long messageSendInterval = commandLineOpt.getMessageSendInterval();
        LOGGER.info("------ Flink Benchmark Data Generator ------");
        LOGGER.info(" Bootstrap Servers: {}", commandLineOpt.getBootstrapServers());
        LOGGER.info(" Kafka Topic: {}", commandLineOpt.getTopic());
        LOGGER.info(" Number of Partitions: {}", commandLineOpt.getNumberOfThreads());
        if (messageSendInterval > 0) {
            LOGGER.info(" Interval: {}ms", messageSendInterval);
        } else {
            LOGGER.info(" Interval: No Interval");
        }
        LOGGER.info(" Payload: {}", commandLineOpt.getPayloadType());
        if (messageSendInterval > 0) {
            LOGGER.info(" Estimated speed: {} records/s", commandLineOpt.getNumberOfThreads() * 1000L / messageSendInterval);
        }
        LOGGER.info(" Data Generator will be running for {} minutes", RUNNING_DURATION_MINUTE);
    }
}
