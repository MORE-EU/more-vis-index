package eu.more2020.visual.domain;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Map;

public class QueryResults implements Serializable {

    private static final long serialVersionUID = 1L;

    private Map<Integer, List<UnivariateDataPoint>> data;

    private Map<Integer, DoubleSummaryStatistics> measureStats;

    private List<LocalDateTime> timeRange = new ArrayList<>();

    private int ioCount = 0;

    public List<LocalDateTime> getTimeRange() {
        return this.timeRange;
    }

    public void setTimeRange(List<LocalDateTime> timeRange) {
        this.timeRange = timeRange;
    }

    public Map<Integer, List<UnivariateDataPoint>> getData() {
        return data;
    }

    public void setData(Map<Integer, List<UnivariateDataPoint>> data) {
        this.data = data;
    }

    public Map<Integer, DoubleSummaryStatistics> getMeasureStats() {
        return measureStats;
    }

    public void setMeasureStats(Map<Integer, DoubleSummaryStatistics> measureStats) {
        this.measureStats = measureStats;
    }

    public int getIoCount() {
        return ioCount;
    }

    public void setIoCount(int ioCount) {
        this.ioCount = ioCount;
    }

    @Override
    public String toString() {
        return "QueryResults{" +
            "data=" + data +
            ", measureStats=" + measureStats +
            ", timeRange=" + timeRange +
            ", ioCount=" + ioCount +
            '}';
    }
}
