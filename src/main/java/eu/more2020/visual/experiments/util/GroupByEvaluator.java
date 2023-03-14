package eu.more2020.visual.experiments.util;

import eu.more2020.visual.domain.AggregatedDataPoint;
import eu.more2020.visual.domain.StatsAggregator;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class GroupByEvaluator implements Consumer<AggregatedDataPoint> {

    private ChronoField groupΒyField;

    private Map<Integer, StatsAggregator> groupByResults;

    private ZoneId zoneId = ZoneId.of("UTC");

    private List<Integer> measures;

    public GroupByEvaluator(List<Integer> measures, ChronoField groupΒyField) {
        this.groupΒyField = groupΒyField;
        this.groupByResults = new HashMap<>();
        this.measures = measures;
    }

    public GroupByEvaluator(List<Integer> measures, ChronoField groupΒyField, ZoneId zoneId) {
        this(measures, groupΒyField);
        this.zoneId = zoneId;
    }


    private int getGroupKey(long timestamp, ChronoField groupingField) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        ZonedDateTime zonedDateTime = instant.atZone(ZoneId.systemDefault());
        int groupKey = zonedDateTime.get(groupingField);
        return groupKey;
    }

    public Map<Integer, StatsAggregator> getGroupByResults() {
        return groupByResults;
    }

    public void setZoneId(ZoneId zoneId) {
        this.zoneId = zoneId;
    }


    @Override
    public void accept(AggregatedDataPoint dataPoint) {
        long timestamp = dataPoint.getTimestamp();
        int groupKey = getGroupKey(timestamp, groupΒyField);
        StatsAggregator statsAggregator = groupByResults.getOrDefault(groupKey, new StatsAggregator(measures));
        statsAggregator.accept(dataPoint);
    }
}
