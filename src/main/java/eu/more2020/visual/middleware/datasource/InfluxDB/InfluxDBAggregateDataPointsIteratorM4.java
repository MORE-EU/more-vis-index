package eu.more2020.visual.middleware.datasource.InfluxDB;

import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import eu.more2020.visual.middleware.datasource.DataSource;
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

    private static final Logger LOG = LoggerFactory.getLogger(DataSource.class);

    private final List<List<Integer>> measures;
    private final List<List<String>> measureNames;
    private int current;
    private long groupTimestamp, currentGroupTimestamp;
    private long endTimestamp, currentEndTimestamp;

    private int i = 0;

    private final Integer numberOfTables;
    private int currentTable;
    private int currentSize;
    private final Integer numberOfGroups;
    private List<FluxRecord> currentRecords;
    private final List<FluxTable> tables;
    StatsAggregator statsAggregator;

    public InfluxDBAggregateDataPointsIteratorM4(List<List<String>> measureNames, List<List<Integer>> measures, List<FluxTable> tables, Integer numberOfGroups) {
        this.measures = measures;
        this.measureNames = measureNames;
        this.currentTable = 0;
        this.tables = tables;
        this.currentRecords = tables.get(currentTable).getRecords();
        this.currentSize = this.currentRecords.size();
        this.numberOfGroups = numberOfGroups;
        this.numberOfTables = tables.size();
        this.current = 0;
        if (!currentRecords.isEmpty()) {
            groupTimestamp = ((Instant) currentRecords.get(current).getValues().get("_start")).toEpochMilli();
            endTimestamp = ((Instant) currentRecords.get(current).getValues().get("_stop")).toEpochMilli();
            currentGroupTimestamp = groupTimestamp;
            currentEndTimestamp = endTimestamp;
        }
    }

    @Override
    public boolean hasNext() {
        if(current < currentSize) return true;
        else{
            if(currentTable < numberOfTables - 1) {
                current = 0;
                currentTable ++;
                currentRecords = tables.get(currentTable).getRecords();
                currentSize = currentRecords.size();
                if (!currentRecords.isEmpty()) {
                    groupTimestamp = ((Instant) currentRecords.get(current).getValues().get("_start")).toEpochMilli();
                    endTimestamp = ((Instant) currentRecords.get(current).getValues().get("_stop")).toEpochMilli();
                    currentGroupTimestamp = groupTimestamp;
                    currentEndTimestamp = endTimestamp;
                }
                return true;
            }
            else return false;
        }
    }

    @Override
    public AggregatedDataPoint next() {
        if (!hasNext()) throw new NoSuchElementException("No more elements to iterate over");
        return createAggregatedDataPoint();
    }

    private AggregatedDataPoint createAggregatedDataPoint() {
        statsAggregator = new StatsAggregator(measures.get(currentTable));
        while (hasNext() && currentGroupTimestamp == groupTimestamp) {
            FluxRecord record = currentRecords.get(current);
            int measure = measures.get(currentTable).get(measureNames.get(currentTable).indexOf(record.getField()));
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
        LOG.debug("Creating agg datapoint from InfluxDB with agg interval {}: {}", aggregatedDataPoint.getTo() - aggregatedDataPoint.getFrom(), aggregatedDataPoint);
        groupTimestamp = currentGroupTimestamp;
        endTimestamp = currentEndTimestamp;
        i++;
        return aggregatedDataPoint;
    }
}
