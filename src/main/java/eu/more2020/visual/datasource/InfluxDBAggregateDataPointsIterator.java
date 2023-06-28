package eu.more2020.visual.datasource;

import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import eu.more2020.visual.domain.*;
import eu.more2020.visual.util.DateTimeUtil;
import org.apache.parquet.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

public class InfluxDBAggregateDataPointsIterator implements Iterator<AggregatedDataPoint> {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBAggregateDataPointsIterator.class);

    private final List<Integer> measures;
    private final List<String> measureNames;
    private int current;
    private long groupTimestamp, currentGroupTimestamp;
    private long endTimestamp, currentEndTimestamp;

    private int i = 0;

    private final Integer numberOfGroups;
    private final Integer numberOfTables;
    private int currentTable;
    private int currentSize;
    private List<FluxRecord> currentRecords;
    private final List<FluxTable> tables;

    public InfluxDBAggregateDataPointsIterator(List<String> measureNames, List<Integer> measures, List<FluxTable> tables, Integer numberOfGroups) {
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
        NonTimestampedStatsAggregator statsAggregator = new NonTimestampedStatsAggregator(measures);
        while (hasNext() && currentGroupTimestamp == groupTimestamp) {
            FluxRecord record = currentRecords.get(current);
            int measure = measures.get(measureNames.indexOf(record.getField()));
            currentGroupTimestamp = record.getTime().toEpochMilli();
            if (currentGroupTimestamp != groupTimestamp) {
                break;
            }
            if(record.getValue() != null) { // check for empty value
                double value = (double) record.getValue();
                statsAggregator.accept(value, measure);
            }
            current++;
        }
        if(current == currentSize){
            currentGroupTimestamp = endTimestamp;
        }
        statsAggregator.setFrom(groupTimestamp);
        statsAggregator.setTo(currentGroupTimestamp);
        AggregatedDataPoint aggregatedDataPoint = new ImmutableAggregatedDataPoint(groupTimestamp, currentGroupTimestamp, statsAggregator);
//        LOG.debug("Created aggregate Datapoint {} - {} ", DateTimeUtil.format(groupTimestamp), DateTimeUtil.format(currentGroupTimestamp));
        groupTimestamp = currentGroupTimestamp;
        i++;
        return aggregatedDataPoint;
    }

}
