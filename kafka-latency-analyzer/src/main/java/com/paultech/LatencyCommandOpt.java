package com.paultech;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class LatencyCommandOpt {
    private static final Logger LOGGER = LoggerFactory.getLogger(LatencyCommandOpt.class);

    // Command line options
    private static final String BOOTSTRAP_SERVERS = "bootstrap-servers";
    private static final String TOPIC = "topic";
    private static final String GROUP_ID = "group-id";
    public static final String HELP = "help";
    // Kafka properties
    private String bootstrapServers;
    private String topic;
    private String groupId;
    private String enableAutoCommit = "true";
    private String autoCommitIntervalMs = "1000";
    private String keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
    private String valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
    private Integer sampleSize;

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

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getEnableAutoCommit() {
        return enableAutoCommit;
    }

    public void setEnableAutoCommit(String enableAutoCommit) {
        this.enableAutoCommit = enableAutoCommit;
    }

    public String getAutoCommitIntervalMs() {
        return autoCommitIntervalMs;
    }

    public void setAutoCommitIntervalMs(String autoCommitIntervalMs) {
        this.autoCommitIntervalMs = autoCommitIntervalMs;
    }

    public String getKeyDeserializer() {
        return keyDeserializer;
    }

    public void setKeyDeserializer(String keyDeserializer) {
        this.keyDeserializer = keyDeserializer;
    }

    public String getValueDeserializer() {
        return valueDeserializer;
    }

    public void setValueDeserializer(String valueDeserializer) {
        this.valueDeserializer = valueDeserializer;
    }

    public Integer getSampleSize() {
        return sampleSize;
    }

    public void setSampleSize(Integer sampleSize) {
        this.sampleSize = sampleSize;
    }

    private static Options buildOptions() {
        Options options = new Options();

        options.addOption("b", BOOTSTRAP_SERVERS, true, "Bootstrap servers");
        options.addOption("t", TOPIC, true, "Topic");
        options.addOption("g", GROUP_ID, true, "Group ID");
        options.addOption("h", HELP, false, "Get help message");
        return options;
    }

    public static LatencyCommandOpt parseCommandLine(String[] args) {
        CommandLineParser commandLineParser = new DefaultParser();
        LatencyCommandOpt latencyCommandOpt = new LatencyCommandOpt();
        try {
            Options buildOptions = buildOptions();
            CommandLine commandLine = commandLineParser.parse(buildOptions, args);
            if (commandLine.hasOption("h")) {
                HelpFormatter helpFormatter = new HelpFormatter();
                helpFormatter.printHelp("-b -t -g", buildOptions);
                System.exit(0);
            }
            if (null == commandLine.getOptionValue(BOOTSTRAP_SERVERS)) {
                LOGGER.error("Option bootstrap servers is required. Exit");
                System.exit(0);
            }

            if (null == commandLine.getOptionValue(TOPIC)) {
                LOGGER.error("Option bootstrap servers is required. Exit");
                System.exit(0);
            }
            latencyCommandOpt.bootstrapServers = commandLine.getOptionValue("bootstrap-servers");
            latencyCommandOpt.topic = commandLine.getOptionValue("topic");
            latencyCommandOpt.groupId = commandLine.getOptionValue("group-id", "default-group");
        } catch (ParseException e) {
            LOGGER.error(e.getMessage());
        }
        return latencyCommandOpt;
    }

    public Properties buildKafkaProperties() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServers);
        props.setProperty("group.id", groupId);
        props.setProperty("enable.auto.commit", enableAutoCommit);
        props.setProperty("auto.commit.interval.ms", autoCommitIntervalMs);
        props.setProperty("key.deserializer", keyDeserializer);
        props.setProperty("value.deserializer", valueDeserializer);
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("session.timeout.ms", "30000");
        return props;
    }

    @Override
    public String toString() {
        return "LatencyCommandOpt{" +
            "bootstrapServers='" + bootstrapServers + '\'' +
            ", topic='" + topic + '\'' +
            ", groupId='" + groupId + '\'' +
            ", enableAutoCommit='" + enableAutoCommit + '\'' +
            ", autoCommitIntervalMs='" + autoCommitIntervalMs + '\'' +
            ", keyDeserializer='" + keyDeserializer + '\'' +
            ", valueDeserializer='" + valueDeserializer + '\'' +
            ", sampleSize=" + sampleSize +
            '}';
    }
}
