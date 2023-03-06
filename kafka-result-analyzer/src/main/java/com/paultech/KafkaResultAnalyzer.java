package com.paultech;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Latency and Throughput Analyzer
 */
public class KafkaResultAnalyzer {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaResultAnalyzer.class);
    private static final Histogram histogram = new Histogram(new UniformReservoir());

    public static void main(String[] args) {
        ResultCommandOpt resultCommandOpt = ResultCommandOpt.parseCommandLine(args);
        LOGGER.info(resultCommandOpt.toString());

        int numberOfAnalyzer = getNumberOfAnalyzer(resultCommandOpt);
        if (numberOfAnalyzer == 0) {
            LOGGER.error("Cannot get number of partitions for topic: {}. Exit.", resultCommandOpt.getTopic());
        }

        ExecutorService executorService = Executors.newFixedThreadPool(numberOfAnalyzer);
        List<AnalyzerTask> analyzerGroup = createAnalyzerGroup(resultCommandOpt, numberOfAnalyzer);

        for (AnalyzerTask analyzerTask : analyzerGroup) {
            executorService.submit(analyzerTask);
        }

        executorService.shutdown();

        try {
            if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                closeAnalyzerGroup(analyzerGroup);
                Thread.sleep(1500);
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Long throughput = AnalyzerResult.calculateThroughput(analyzerGroup.stream().map(AnalyzerTask::getAnalyzerResult).collect(Collectors.toList()));
        Snapshot snapshot = histogram.getSnapshot();
        printReport(snapshot, throughput);
    }

    private static List<AnalyzerTask> createAnalyzerGroup(ResultCommandOpt resultCommandOpt, int numberOfAnalyzers) {
        List<AnalyzerTask> analyzerTaskList = new ArrayList<>(numberOfAnalyzers);
        for (int i = 0; i < numberOfAnalyzers; i++) {
            analyzerTaskList.add(new AnalyzerTask(resultCommandOpt, histogram));
        }
        return analyzerTaskList;
    }

    private static void closeAnalyzerGroup(List<AnalyzerTask> analyzerTaskList) {
        for (AnalyzerTask analyzerTask : analyzerTaskList) {
            analyzerTask.close();
        }
    }

    private static int getNumberOfAnalyzer(ResultCommandOpt resultCommandOpt) {
        int numberOfAnalyzer = 0;
        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(resultCommandOpt.buildKafkaProperties())) {
            numberOfAnalyzer = kafkaConsumer.partitionsFor(resultCommandOpt.getTopic()).size();
        }
        return numberOfAnalyzer;
    }

    private static void printReport(Snapshot snapshot, Long throughput) {
        LOGGER.info("---------------------------------------------------------");
        LOGGER.info("Report");
        LOGGER.info("75 Percentile: {}ms", snapshot.get75thPercentile());
        LOGGER.info("95 Percentile: {}ms", snapshot.get95thPercentile());
        LOGGER.info("99 Percentile: {}ms", snapshot.get99thPercentile());
        LOGGER.info("999 Percentile: {}ms", snapshot.get999thPercentile());
        LOGGER.info("Median: {}", snapshot.getMedian());
        LOGGER.info("MAX: {}", snapshot.getMax());
        LOGGER.info("MIN: {}", snapshot.getMin());
        LOGGER.info("Throughput(records/s): {}", throughput);
        LOGGER.info("---------------------------------------------------------");
    }
}
