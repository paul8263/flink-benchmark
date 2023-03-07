package com.paultech;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Generate Kafka test data
 */
public class KafkaDataGen {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDataGen.class);

    public static final long RUNNING_DURATION_MS = 600000L;
    public static void main(String[] args) throws InterruptedException {
        CommandLineOpt commandLineOpt = CommandLineOpt.parseCommandLine(args);
        LOGGER.info(commandLineOpt.toString());

//        createTopic(commandLineOpt);

        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(commandLineOpt.getNumberOfThreads());
        List<KafkaMessageSender> kafkaMessageSenders = generateKafkaMessageSenders(commandLineOpt);
        for (KafkaMessageSender kafkaMessageSender : kafkaMessageSenders) {
            scheduledExecutorService.scheduleAtFixedRate(kafkaMessageSender, 0, commandLineOpt.getMessageSendInterval(), TimeUnit.MILLISECONDS);
        }
        logInfo(commandLineOpt);

        Thread.sleep(RUNNING_DURATION_MS);

        scheduledExecutorService.shutdown();
        if (!scheduledExecutorService.awaitTermination(3000, TimeUnit.MILLISECONDS)) {
            scheduledExecutorService.shutdownNow();
        }
        closeKafkaMessageSenders(kafkaMessageSenders);
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

    private static List<KafkaMessageSender> generateKafkaMessageSenders(CommandLineOpt commandLineOpt) {
        int numberOfThreads = commandLineOpt.getNumberOfThreads();
        List<KafkaMessageSender> kafkaMessageSenderList = new ArrayList<>(numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            kafkaMessageSenderList.add(new KafkaMessageSender(commandLineOpt));
        }
        return kafkaMessageSenderList;
    }

    private static void closeKafkaMessageSenders(List<KafkaMessageSender> kafkaMessageSenderList) {
        for (KafkaMessageSender kafkaMessageSender : kafkaMessageSenderList) {
            kafkaMessageSender.close();
        }
    }

    private static void logInfo(CommandLineOpt commandLineOpt) {
        long messageSendInterval = commandLineOpt.getMessageSendInterval();
        int messagesPerInterval = commandLineOpt.getMessagesPerInterval();
        LOGGER.info("------ Flink Benchmark Data Generator ------");
        LOGGER.info(" Bootstrap Servers: {}", commandLineOpt.getBootstrapServers());
        LOGGER.info(" Kafka Topic: {}", commandLineOpt.getTopic());
        LOGGER.info(" Number of Partitions: {}", commandLineOpt.getNumberOfThreads());
        LOGGER.info(" Interval: {}ms", messageSendInterval);
        LOGGER.info(" Messages per interval: {}", messagesPerInterval);
        LOGGER.info(" Payload: {}", commandLineOpt.getPayloadType());
        LOGGER.info(" Estimated speed: {} records/s", commandLineOpt.getNumberOfThreads() * messagesPerInterval * 1000L / messageSendInterval);

        LOGGER.info(" Data Generator will be running for {} ms", RUNNING_DURATION_MS);
    }
}
