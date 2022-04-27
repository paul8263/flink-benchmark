package com.paultech;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Properties;

public class CommandLineOpt {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommandLineOpt.class);

    // Command line properties
    public static final String BOOTSTRAP_SERVERS = "bootstrapServers";
    public static final String TOPIC = "topic";
    public static final String ACKS = "acks";
    public static final String NUMBER_OF_THREADS = "threads";
    public static final String MESSAGE_SEND_INTERVAL = "messageSendInterval";
    public static final String PAYLOAD_TYPE = "payloadType";
    public static final String HELP = "help";

    // Kafka properties
    private String bootstrapServers;
    private String topic;
    private String acks = "0";
    private int batchSize = 16384;
    private int lingerms = 32;
    private int retries = 0;
    private int bufferMemory = -1;
    private String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";
    private String valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";

    // Datagen properties
    private int numberOfThreads = 1;
    private long messageSendInterval = -1;

    private String payloadType = "UUID";

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

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getLingerms() {
        return lingerms;
    }

    public void setLingerms(int lingerms) {
        this.lingerms = lingerms;
    }

    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public int getBufferMemory() {
        return bufferMemory;
    }

    public void setBufferMemory(int bufferMemory) {
        this.bufferMemory = bufferMemory;
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

    public String getPayloadType() {
        return payloadType;
    }

    public void setPayloadType(String payloadType) {
        this.payloadType = payloadType;
    }

    private static Options buildOptions() {
        Options options = new Options();
        options.addOption("b", BOOTSTRAP_SERVERS, true, "Bootstrap Servers");
        options.addOption("t", TOPIC, true, "Topic");
        options.addOption("a", ACKS, true, "Acks");
        options.addOption("n", NUMBER_OF_THREADS, true, "Number of threads");
        options.addOption("i", MESSAGE_SEND_INTERVAL, true, "Message send interval");
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

            if (null == commandLine.getOptionValue(BOOTSTRAP_SERVERS)) {
                LOGGER.error("Option bootstrap servers is required. Exit");
                System.exit(-1);
            }
            if (null == commandLine.getOptionValue(TOPIC)) {
                LOGGER.error("Option topic is required. Exit");
                System.exit(-1);
            }
            commandLineOpt.bootstrapServers = commandLine.getOptionValue(BOOTSTRAP_SERVERS);
            commandLineOpt.topic = commandLine.getOptionValue(TOPIC);

            Optional.ofNullable(commandLine.getOptionValue(ACKS)).ifPresent((acks) -> {
                commandLineOpt.acks = acks;
            });
            Optional.ofNullable(commandLine.getOptionValue(NUMBER_OF_THREADS)).ifPresent((numberOfThreads) -> {
                commandLineOpt.numberOfThreads = Integer.parseInt(numberOfThreads);
            });
            Optional.ofNullable(commandLine.getOptionValue(MESSAGE_SEND_INTERVAL)).ifPresent((interval) -> {
                commandLineOpt.messageSendInterval = Long.parseLong(interval);
            });
            Optional.ofNullable(commandLine.getOptionValue(PAYLOAD_TYPE)).ifPresent((payloadType) -> {
                commandLineOpt.payloadType = payloadType;
            });
        } catch (ParseException e) {
            LOGGER.error(e.toString());
        }
        return commandLineOpt;
    }

    public Properties buildKafkaProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("acks", acks);
        properties.put("retries", retries);
        properties.put("batch.size", batchSize);
        properties.put("linger.ms", lingerms);
        if (bufferMemory > 0) {
            properties.put("buffer.memory", bufferMemory);
        }
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
            ", batchSize=" + batchSize +
            ", lingerms=" + lingerms +
            ", retries=" + retries +
            ", bufferMemory=" + bufferMemory +
            ", keySerializer='" + keySerializer + '\'' +
            ", valueSerializer='" + valueSerializer + '\'' +
            ", numberOfThreads=" + numberOfThreads +
            ", messageSendInterval=" + messageSendInterval +
            ", payloadType='" + payloadType + '\'' +
            '}';
    }
}
