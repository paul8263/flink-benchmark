package com.paultech;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class CommandLineOpt {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommandLineOpt.class);

    // Command line properties
    public static final String BOOTSTRAP_SERVERS = "bootstrapServers";
    public static final String TOPIC = "topic";
    public static final String ACKS = "acks";
    public static final String NUMBER_OF_THREADS = "threads";
    public static final String MESSAGE_SEND_INTERVAL = "messageSendInterval";
    private static final String MESSAGES_PER_INTERVAL = "messagesPerInterval";
    public static final String PAYLOAD_TYPE = "payloadType";
    public static final String HELP = "help";

    // Kafka properties
    private String bootstrapServers;
    private String topic;
    private String acks = "0";
    private String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";
    private String valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";

    // Datagen properties
    private int numberOfThreads = 1;
    private long messageSendInterval = -1;

    private int messagesPerInterval = 5;

    private PayloadType payloadType;

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getAcks() {
        return acks;
    }

    public void setAcks(String acks) {
        this.acks = acks;
    }

    public String getKeySerializer() {
        return keySerializer;
    }

    public void setKeySerializer(String keySerializer) {
        this.keySerializer = keySerializer;
    }

    public String getValueSerializer() {
        return valueSerializer;
    }

    public void setValueSerializer(String valueSerializer) {
        this.valueSerializer = valueSerializer;
    }

    public int getNumberOfThreads() {
        return numberOfThreads;
    }

    public void setNumberOfThreads(int numberOfThreads) {
        this.numberOfThreads = numberOfThreads;
    }

    public long getMessageSendInterval() {
        return messageSendInterval;
    }

    public void setMessageSendInterval(long messageSendInterval) {
        this.messageSendInterval = messageSendInterval;
    }

    public PayloadType getPayloadType() {
        return payloadType;
    }

    public void setPayloadType(PayloadType payloadType) {
        this.payloadType = payloadType;
    }

    public int getMessagesPerInterval() {
        return messagesPerInterval;
    }

    public void setMessagesPerInterval(int messagesPerInterval) {
        this.messagesPerInterval = messagesPerInterval;
    }

    private static Options buildOptions() {
        Options options = new Options();
        options.addOption("b", BOOTSTRAP_SERVERS, true, "Bootstrap Servers");
        options.addOption("t", TOPIC, true, "Topic");
        options.addOption("a", ACKS, true, "Acks");
        options.addOption("n", NUMBER_OF_THREADS, true, "Number of threads");
        options.addOption("i", MESSAGE_SEND_INTERVAL, true, "Message send interval");
        options.addOption("c", MESSAGES_PER_INTERVAL, true, "Messages per interval");
        options.addOption("p", PAYLOAD_TYPE, true, "Kafka data payload type");
        options.addOption("h", HELP, false, "Get help message");
        return options;
    }

    public static CommandLineOpt parseCommandLine(String[] args) {
        CommandLineParser parser = new DefaultParser();
        CommandLineOpt commandLineOpt = new CommandLineOpt();
        try {
            Options buildOptions = buildOptions();
            CommandLine commandLine = parser.parse(buildOptions, args);
            if (commandLine.hasOption("h")) {
                HelpFormatter helpFormatter = new HelpFormatter();
                helpFormatter.printHelp("-b -t -a -n -i -p", buildOptions);
                System.exit(0);
            }
            commandLineOpt.bootstrapServers = commandLine.getOptionValue(BOOTSTRAP_SERVERS);
            commandLineOpt.topic = commandLine.getOptionValue(TOPIC);
            commandLineOpt.acks = commandLine.getOptionValue(ACKS, "0");
            commandLineOpt.numberOfThreads = Integer.parseInt(commandLine.getOptionValue(NUMBER_OF_THREADS, "1"));
            commandLineOpt.messageSendInterval = Long.parseLong(commandLine.getOptionValue(MESSAGE_SEND_INTERVAL, "10"));
            commandLineOpt.payloadType = PayloadType.of(commandLine.getOptionValue(PAYLOAD_TYPE, "uuid"));
            commandLineOpt.messagesPerInterval = Integer.parseInt(commandLine.getOptionValue(MESSAGES_PER_INTERVAL, "5"));
        } catch (ParseException e) {
            LOGGER.error(e.toString());
            System.exit(-1);
        }
        checkParameters(commandLineOpt);
        return commandLineOpt;
    }

    private static void checkParameters(CommandLineOpt commandLineOpt) {
        if (null == commandLineOpt.getBootstrapServers()) {
            LOGGER.error("Option bootstrap servers is required. Exit");
            System.exit(-1);
        }
        if (null == commandLineOpt.getTopic()) {
            LOGGER.error("Option topic is required. Exit");
            System.exit(-1);
        }

        if (null == commandLineOpt.getPayloadType()) {
            LOGGER.error("Payload type error. Exit");
            System.exit(-1);
        }

        if (commandLineOpt.messageSendInterval <= 0) {
            LOGGER.error("Messages send interval should be greater than 0. Exit");
            System.exit(-1);
        }
    }

    public Properties buildKafkaProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("acks", acks);
        properties.put("key.serializer", keySerializer);
        properties.put("value.serializer", valueSerializer);
        return properties;
    }

    @Override
    public String toString() {
        return "CommandLineOpt{" +
            "bootstrapServers='" + bootstrapServers + '\'' +
            ", topic='" + topic + '\'' +
            ", acks='" + acks + '\'' +
            ", keySerializer='" + keySerializer + '\'' +
            ", valueSerializer='" + valueSerializer + '\'' +
            ", numberOfThreads=" + numberOfThreads +
            ", messageSendInterval=" + messageSendInterval +
            ", messagesPerInterval=" + messagesPerInterval +
            ", payloadType=" + payloadType +
            '}';
    }
}
