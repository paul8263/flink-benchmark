package com.paultech;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KafkaLatencyAnalyzer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaLatencyAnalyzer.class);

    private static final int NUMBER_OF_ANALYZER = 4;

    private static Histogram histogram;

    public static void main(String[] args) {

        LatencyCommandOpt latencyCommandOpt = LatencyCommandOpt.parseCommandLine(args);
        LOGGER.info(latencyCommandOpt.toString());
        Properties properties = new KafkaConsumerConfig(latencyCommandOpt).toProperties();
        LOGGER.info(properties.toString());

        histogram = new Histogram(new UniformReservoir(9999999));

        ExecutorService executorService = Executors.newFixedThreadPool(NUMBER_OF_ANALYZER);

        List<AnalyzerTask> analyzerGroup = createAnalyzerGroup(latencyCommandOpt.getTopic(), properties);

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
            Snapshot snapshot = histogram.getSnapshot();
            printReport(snapshot);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private static List<AnalyzerTask> createAnalyzerGroup(String topic, Properties properties) {
        List<AnalyzerTask> analyzerTaskList = new ArrayList<>(NUMBER_OF_ANALYZER);
        for (int i = 0; i < NUMBER_OF_ANALYZER; i++) {
            analyzerTaskList.add(new AnalyzerTask(topic, properties, histogram));
        }
        return analyzerTaskList;
    }

    private static void closeAnalyzerGroup(List<AnalyzerTask> analyzerTaskList) {
        for (AnalyzerTask analyzerTask : analyzerTaskList) {
            analyzerTask.close();
        }
    }

    private static void printReport(Snapshot snapshot) {
        LOGGER.info("---------------------------------------------------------");
        LOGGER.info("Latency Report");
        LOGGER.info("75 Percentile: {}", snapshot.get75thPercentile());
        LOGGER.info("95 Percentile: {}", snapshot.get95thPercentile());
        LOGGER.info("99 Percentile: {}", snapshot.get99thPercentile());
        LOGGER.info("999 Percentile: {}", snapshot.get999thPercentile());
        LOGGER.info("Median: {}", snapshot.getMedian());
        LOGGER.info("MAX: {}", snapshot.getMax());
        LOGGER.info("MIN: {}", snapshot.getMin());
        LOGGER.info("---------------------------------------------------------");
    }

}
