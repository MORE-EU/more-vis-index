package eu.more2020.visual.middleware.datasource;

import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import eu.more2020.visual.middleware.domain.AggregatedDataPoint;
import eu.more2020.visual.middleware.domain.ImmutableAggregatedDataPoint;
import eu.more2020.visual.middleware.domain.StatsAggregator;
import eu.more2020.visual.middleware.domain.UnivariateDataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

public class InfluxDBAggregateDataPointsIteratorM4 implements Iterator<AggregatedDataPoint> {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBAggregateDataPointsIteratorM4.class);

    private final List<Integer> measures;
    private final List<String> measureNames;
    private final List<FluxRecord> records;
    private final int size;
    private int current;
    private long groupTimestamp, currentGroupTimestamp;
    private long endTimestamp, currentEndTimestamp;

    private int i = 0;

    private final Integer numberOfGroups;

    public InfluxDBAggregateDataPointsIteratorM4(List<String> measureNames, List<Integer> measures, FluxTable table, Integer numberOfGroups) {
        this.measures = measures;
        this.measureNames = measureNames;
        this.records = table.getRecords();
        this.size = this.records.size();
        this.numberOfGroups = numberOfGroups;
        this.current = 0;
        if (!records.isEmpty()) {
            groupTimestamp = ((Instant) records.get(current).getValues().get("_start")).toEpochMilli();
            endTimestamp = ((Instant) records.get(current).getValues().get("_stop")).toEpochMilli();
            currentGroupTimestamp = groupTimestamp;
            currentEndTimestamp = endTimestamp;
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
            int measure = measures.get(measureNames.indexOf(record.getField()));
            long timestamp = Objects.requireNonNull(record.getTime()).toEpochMilli();
            if(record.getValue() != null) { // check for empty value
                double value = (double) record.getValue();
                currentGroupTimestamp = ((Instant) record.getValues().get("_start")).toEpochMilli();
                currentEndTimestamp = ((Instant) record.getValues().get("_stop")).toEpochMilli();
                if (currentGroupTimestamp != groupTimestamp) {
                    break;
                }
                UnivariateDataPoint point = new UnivariateDataPoint(timestamp, value);
                statsAggregator.accept(point, measure);
            }
            current++;
        }

        AggregatedDataPoint aggregatedDataPoint = new ImmutableAggregatedDataPoint(groupTimestamp, endTimestamp, statsAggregator);
//        LOG.debug("Creating agg datapoint from InfluxDB with agg interval {}: {}", aggregatedDataPoint.getTo() - aggregatedDataPoint.getFrom(), aggregatedDataPoint);
        groupTimestamp = currentGroupTimestamp;
        endTimestamp = currentEndTimestamp;
        i++;
        return aggregatedDataPoint;
    }
}