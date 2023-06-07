package eu.more2020.visual.datasource;

import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import eu.more2020.visual.domain.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

public class InfluxDBAggregateDataPointsIterator implements Iterator<AggregatedDataPoint> {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBAggregateDataPointsIterator.class);

    private final List<Integer> measures;
    private final List<String> measureNames;
    private final List<FluxRecord> records;
    private final int size;
    private int current;
    private long groupTimestamp, currentGroupTimestamp;
    private long endTimestamp;

    private final Integer numberOfGroups;

    public InfluxDBAggregateDataPointsIterator(List<String> measureNames, List<Integer> measures, FluxTable table, Integer numberOfGroups) {
        this.measures = measures;
        this.measureNames = measureNames;
        this.records = table.getRecords();
        this.size = this.records.size();
        this.numberOfGroups = numberOfGroups;
        this.current = 0;
        if (!records.isEmpty()) {
            groupTimestamp = ((Instant) records.get(current).getValues().get("_start")).toEpochMilli();
            currentGroupTimestamp = groupTimestamp;
        }
    }

    @Override
    public boolean hasNext() {
        return current < size;
    }

    @Override
    public AggregatedDataPoint next() {
        if (!hasNext()) throw new NoSuchElementException("No more elements to iterate over");
        return createAggregatedDataPoint();
    }

    private AggregatedDataPoint createAggregatedDataPoint() {
        StatsAggregator statsAggregator = new StatsAggregator(measures);


        while (hasNext() && currentGroupTimestamp == groupTimestamp) {
            FluxRecord record = records.get(current);
            int measureId = measureNames.indexOf(record.getField());
            long timestamp = Objects.requireNonNull(record.getTime()).toEpochMilli();
            double value = (double) record.getValue();
            currentGroupTimestamp = ((Instant) record.getValues().get("_start")).toEpochMilli();
            endTimestamp = ((Instant) record.getValues().get("_stop")).toEpochMilli();
            StringBuilder stringBuilder = new StringBuilder();
            record.getValues().forEach((k, v) -> stringBuilder.append(k).append(": ").append(v).append(", "));
            LOG.debug("Agg Interval: {} Record: {}", endTimestamp - currentGroupTimestamp, stringBuilder);
            UnivariateDataPoint point = new UnivariateDataPoint(timestamp, value);
            statsAggregator.accept(point, measureId);
            current++;
        }

        // todo: we need to check if the end timestamp is right here and matches the ones from the time series span
        AggregatedDataPoint dataPoint = new ImmutableAggregatedDataPoint(groupTimestamp, endTimestamp, statsAggregator);
        groupTimestamp = currentGroupTimestamp;
        return dataPoint;
    }

}
