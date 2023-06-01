package eu.more2020.visual.domain;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

public class PixelColumn {

    ZonedDateTime from;
    ZonedDateTime to;
    boolean partialLeft = false;
    boolean partialRight = false;
    List<AggregatedDataPoint> aggregatedDataPoints;
    PixelStatsAggregator stats;

    public PixelColumn(ZonedDateTime from, ZonedDateTime to) {
        this.from = from;
        this.to = to;
        aggregatedDataPoints = new ArrayList<>();

    }

    public ZonedDateTime getFrom() {
        return from;
    }

    public ZonedDateTime getTo() {
        return to;
    }

    public boolean hasPartialLeft() {
        return partialLeft;
    }

    public boolean hasPartialRight() {
        return partialRight;
    }

    public void setPartialLeft(boolean partialLeft) {
        this.partialLeft = partialLeft;
    }

    public void setPartialRight(boolean partialRight) {
        this.partialRight = partialRight;
    }

    public List<AggregatedDataPoint> getAggregatedDataPoints() {
        return aggregatedDataPoints;
    }

    public void setAggregatedDataPoints(List<AggregatedDataPoint> aggregatedDataPoints) {
        this.aggregatedDataPoints = aggregatedDataPoints;
    }

    public void setStats(PixelStatsAggregator stats) {
        this.stats = stats;
    }

    public PixelStatsAggregator getStats() {
        return stats;
    }
}
