package com.paultech;

import java.util.List;
import java.util.Optional;

public class AnalyzerResult {
    private Long startTime = 0L;

    private Long endTime = 0L;

    private Long count = 0L;

    public AnalyzerResult() {
    }

    public AnalyzerResult(Long startTime, Long endTime, Long count) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.count = count;
    }

    public Long getStartTime() {
        return startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public Long getCount() {
        return count;
    }

    public void update(Long startTime, Long endTime) {
        this.count++;
        if (endTime >= this.startTime) {
            this.startTime = startTime;
        }
        if (startTime <= this.endTime) {
            this.endTime = endTime;
        }
    }

    @Override
    public String toString() {
        return "AnalyzerResult{" +
            "startTime=" + startTime +
            ", endTime=" + endTime +
            ", count=" + count +
            '}';
    }

    public static Long calculateThroughput(List<AnalyzerResult> analyzerResultList) {
        Optional<AnalyzerResult> reducedResult = analyzerResultList.stream().reduce((analyzerResult, analyzerResult2) -> {
            Long startTime = Math.min(analyzerResult.startTime, analyzerResult2.startTime);
            Long endTime = Math.max(analyzerResult.endTime, analyzerResult2.endTime);
            Long count = analyzerResult.count + analyzerResult2.count;
            return new AnalyzerResult(startTime, endTime, count);
        });

        return reducedResult.isPresent() ? reducedResult.get().getCount() : 0L;
    }
}
