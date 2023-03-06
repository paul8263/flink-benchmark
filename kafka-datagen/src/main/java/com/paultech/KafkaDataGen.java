package com.paultech;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
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
    private static void createTopic(CommandLineOpt commandLineOpt) {
        Properties kafkaProperties = commandLineOpt.buildKafkaProperties();
        String topic = commandLineOpt.getTopic();
        try (AdminClient adminClient = AdminClient.create(kafkaProperties)) {
            Map<String, TopicDescription> topicDescriptionMap = adminClient.describeTopics(Collections.singletonList(topic)).all().get();
            if (topicDescriptionMap.containsKey(topic)) {
                LOGGER.info("Topic \"{}\" already exists. Delete it first.", topic);
                adminClient.deleteTopics(Collections.singletonList(topic));
            }

            LOGGER.info("Create topic: {}", topic);
            NewTopic newTopic = new NewTopic(topic, commandLineOpt.getNumberOfThreads(), (short) 1);
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (Exception e) {
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
}
