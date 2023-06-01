package eu.more2020.visual.datasource;

import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import eu.more2020.visual.domain.AggregatedDataPoint;
import eu.more2020.visual.domain.ImmutableAggregatedDataPoint;
import eu.more2020.visual.domain.StatsAggregator;
import eu.more2020.visual.domain.UnivariateDataPoint;
import eu.more2020.visual.util.DateTimeUtil;

import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class InfluxDBAggregateDataPointsIterator implements Iterator<AggregatedDataPoint> {


    private final List<Integer> measures;
    private final List<String> measureNames;

    private final List<FluxRecord> records;
    private int current;
    private long group, currentGroup; // long because it's a timestamp with the stop of the bucket a row belongs too.
    private final int size;

    public InfluxDBAggregateDataPointsIterator(List<String> measureNames, List<Integer> measures, FluxTable table) {
        this.measures = measures;
        this.measureNames = measureNames;
        this.records = table.getRecords();
        this.current = 0;
        this.size = table.getRecords().size();
        this.group = ((Instant) records.get(current).getValues().get("_stop")).toEpochMilli();
        this.currentGroup = group;
    }

    @Override
    public boolean hasNext() {
        return current < size;
    }


    @Override
    public AggregatedDataPoint next() {
        long firstTimestamp = 0L;
        StatsAggregator statsAggregator = new StatsAggregator(measures);
        int i = 0;
        do {
            FluxRecord record = records.get(current);
            int measureId = measureNames.indexOf(record.getField());
            long timestamp = Objects.requireNonNull(record.getTime()).toEpochMilli();
            double value = (double) record.getValue();
            group = ((Instant) record.getValues().get("_stop")).toEpochMilli();
            if (group != currentGroup) {
                break;
            }
            current ++;
            if (i == 0) firstTimestamp = timestamp;
            UnivariateDataPoint point = new UnivariateDataPoint(timestamp, value);
            statsAggregator.accept(point, measureId);
            i++;
        } while(hasNext());
        currentGroup = group;
        return new ImmutableAggregatedDataPoint(firstTimestamp, statsAggregator);
    }
}
