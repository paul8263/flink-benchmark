package com.paultech;

import org.apache.commons.cli.*;

public class CommandLineOpt {

    public static final String BOOTSTRAP_SERVERS = "bootstrapServers";
    public static final String TOPIC = "topic";
    public static final String ACKS = "acks";
    public static final String NUMBER_OF_THREADS = "threads";

    private String bootstrapServers;
    private String topic;
    private String acks;
    private int numberOfThreads;

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

    public int getNumberOfThreads() {
        return numberOfThreads;
    }

    public void setNumberOfThreads(int numberOfThreads) {
        this.numberOfThreads = numberOfThreads;
    }

    private static Options buildOptions() {
        Options options = new Options();
        options.addOption("b", BOOTSTRAP_SERVERS, true, "Bootstrap Servers");
        options.addOption("t", TOPIC, true, "Topic");
        options.addOption("a", ACKS, true, "Acks");
        options.addOption("n", NUMBER_OF_THREADS, true, "Number of threads");
        return options;
    }

    public static CommandLineOpt parseCommandLine(String[] args) {
        CommandLineParser parser = new DefaultParser();
        CommandLineOpt commandLineOpt = new CommandLineOpt();
        try {
            CommandLine commandLine = parser.parse(buildOptions(), args);
            commandLineOpt.bootstrapServers = commandLine.getOptionValue(BOOTSTRAP_SERVERS);
            commandLineOpt.topic = commandLine.getOptionValue(TOPIC);
            commandLineOpt.acks = commandLine.getOptionValue(ACKS);
            commandLineOpt.numberOfThreads = Integer.parseInt(commandLine.getOptionValue(NUMBER_OF_THREADS));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return commandLineOpt;
    }

    @Override
    public String toString() {
        return "CommandLineOpt{" +
                "bootstrapServers='" + bootstrapServers + '\'' +
                ", topic='" + topic + '\'' +
                ", acks='" + acks + '\'' +
                ", numberOfThreads=" + numberOfThreads +
                '}';
    }
}
