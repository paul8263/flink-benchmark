package com.paultech;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LatencyCommandOpt {
    private static final Logger LOGGER = LoggerFactory.getLogger(LatencyCommandOpt.class);

    private String bootstrapServers;
    private String topic;
    private String groupId;
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

    private static Options buildOptions() {
        Options options = new Options();

        options.addOption("b", "bootstrap-servers", true, "Bootstrap servers");
        options.addOption("t", "topic", true, "Topic");
        options.addOption("g", "group-id", true, "Group ID");
        return options;
    }

    public static LatencyCommandOpt parseCommandLine(String[] args) {
        CommandLineParser commandLineParser = new DefaultParser();
        LatencyCommandOpt latencyCommandOpt = new LatencyCommandOpt();
        try {
            CommandLine parse = commandLineParser.parse(buildOptions(), args);
            latencyCommandOpt.bootstrapServers = parse.getOptionValue("bootstrap-servers", "localhost:9092");
            latencyCommandOpt.topic = parse.getOptionValue("topic", "default");
            latencyCommandOpt.groupId = parse.getOptionValue("group-id", "default-group");
        } catch (ParseException e) {
            LOGGER.error(e.getMessage());
        }
        return latencyCommandOpt;
    }

    @Override
    public String toString() {
        return "LatencyCommandOpt{" +
                "bootstrapServers='" + bootstrapServers + '\'' +
                ", topic='" + topic + '\'' +
                ", groupId='" + groupId + '\'' +
                '}';
    }
}
