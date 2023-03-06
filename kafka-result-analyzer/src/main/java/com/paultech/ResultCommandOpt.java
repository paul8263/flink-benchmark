package com.paultech;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ResultCommandOpt {
    private static final Logger LOGGER = LoggerFactory.getLogger(ResultCommandOpt.class);

    // Command line options
    private static final String BOOTSTRAP_SERVERS = "bootstrap-servers";
    private static final String TOPIC = "topic";
    private static final String GROUP_ID = "group-id";
    public static final String HELP = "help";
    // Kafka properties
    private String bootstrapServers;
    private String topic;
    private String groupId;
    private String keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
    private String valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";

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

    private static Options buildOptions() {
        Options options = new Options();

        options.addOption("b", BOOTSTRAP_SERVERS, true, "Bootstrap servers");
        options.addOption("t", TOPIC, true, "Topic");
        options.addOption("g", GROUP_ID, true, "Group ID");
        options.addOption("h", HELP, false, "Get help message");
        return options;
    }

    public static ResultCommandOpt parseCommandLine(String[] args) {
        CommandLineParser commandLineParser = new DefaultParser();
        ResultCommandOpt resultCommandOpt = new ResultCommandOpt();
        try {
            Options buildOptions = buildOptions();
            CommandLine commandLine = commandLineParser.parse(buildOptions, args);
            if (commandLine.hasOption("h")) {
                HelpFormatter helpFormatter = new HelpFormatter();
                helpFormatter.printHelp("-b -t -g", buildOptions);
                System.exit(0);
            }
            resultCommandOpt.bootstrapServers = commandLine.getOptionValue("bootstrap-servers");
            resultCommandOpt.topic = commandLine.getOptionValue("topic");
            resultCommandOpt.groupId = commandLine.getOptionValue("group-id", "result-analyzer-group");
        } catch (ParseException e) {
            LOGGER.error(e.getMessage());
        }
        checkParameters(resultCommandOpt);
        return resultCommandOpt;
    }

    private static void checkParameters(ResultCommandOpt resultCommandOpt) {
        if (null == resultCommandOpt.getBootstrapServers()) {
            LOGGER.error("Option bootstrap servers is required. Exit");
            System.exit(-1);
        }

        if (null == resultCommandOpt.getTopic()) {
            LOGGER.error("Option bootstrap servers is required. Exit");
            System.exit(-1);
        }
    }

    public Properties buildKafkaProperties() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServers);
        props.setProperty("group.id", groupId);
        props.setProperty("key.deserializer", keyDeserializer);
        props.setProperty("value.deserializer", valueDeserializer);
        props.setProperty("session.timeout.ms", "30000");
        return props;
    }

    @Override
    public String toString() {
        return "ResultCommandOpt{" +
            "bootstrapServers='" + bootstrapServers + '\'' +
            ", topic='" + topic + '\'' +
            ", groupId='" + groupId + '\'' +
            ", keyDeserializer='" + keyDeserializer + '\'' +
            ", valueDeserializer='" + valueDeserializer + '\'' +
            '}';
    }
}
