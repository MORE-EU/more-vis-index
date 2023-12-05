package eu.more2020.visual.middleware.datasource.InfluxDB;

import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import eu.more2020.visual.middleware.datasource.DataSource;
import eu.more2020.visual.middleware.domain.*;
import eu.more2020.visual.middleware.util.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Collectors;

public class InfluxDBAggregateDataPointsIteratorM4 implements Iterator<AggregatedDataPoint> {

    private static final Logger LOG = LoggerFactory.getLogger(DataSource.class);

    private final List<Integer> measures;
    private final List<String> measureNames;
    private int current;
    private long groupTimestamp, currentGroupTimestamp;
    private int i = 0;

    private final Integer numberOfTables;
    private int currentTable;
    private int currentSize;
    private List<FluxRecord> currentRecords;
    private final List<FluxTable> tables;
    StatsAggregator statsAggregator;

    public InfluxDBAggregateDataPointsIteratorM4(List<String> measureNames, List<Integer> measures, List<FluxTable> tables) {
        this.measures = measures;
        this.measureNames = measureNames;
        this.currentTable = 0;
        this.tables = tables;
        this.currentRecords = tables.get(currentTable).getRecords();
        this.currentSize = this.currentRecords.size();
        this.numberOfTables = tables.size();
        this.current = 0;
        if (!currentRecords.isEmpty()) {
            groupTimestamp = ((Instant) currentRecords.get(current).getValues().get("_start")).toEpochMilli();
            currentGroupTimestamp = ((Instant) currentRecords.get(current).getValues().get("_stop")).toEpochMilli();
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
                    currentGroupTimestamp = ((Instant) currentRecords.get(current).getValues().get("_stop")).toEpochMilli();;
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



    /*
    * Reads the table every 4 measures (one for each m4 variable)
    * And inputs them into the stats aggregator.
    * The _stop variable represents the end of a grouping.
    * The _time is the time of each m4 variable.
    * The _start is the start of each grouping.
    * */
    private AggregatedDataPoint createAggregatedDataPoint() {
//        statsAggregator = new StatsAggregator(measures.get(currentTable));
//        for(int i = 0; i < measures.get(currentTable).size() * 4; i ++){
//            FluxRecord record = currentRecords.get(current);
//            int measure = measures.get(currentTable).get(measureNames.get(currentTable).indexOf(record.getField()));
//            if(record.getValue() != null) { // check for empty value
//                double value = (double) record.getValue();
//                long timestamp = ((Instant) record.getValues().get("_time")).toEpochMilli();
//                statsAggregator.accept(new UnivariateDataPoint(timestamp, value), measure);
//            }
//            currentGroupTimestamp = ((Instant) currentRecords.get(current).getValues().get("_stop")).toEpochMilli();
//            current++;
//        }
//        AggregatedDataPoint aggregatedDataPoint = new ImmutableAggregatedDataPoint(groupTimestamp, currentGroupTimestamp, statsAggregator);
//        LOG.debug("Created aggregate Datapoint {} - {} with measures {} with firsts: {}, lasts: {}, mins: {} and maxs: {} ",
//                DateTimeUtil.format(groupTimestamp), DateTimeUtil.format(currentGroupTimestamp), statsAggregator.getMeasures(),
//                statsAggregator.getMeasures().stream().map(statsAggregator::getFirstDataPoint).collect(Collectors.toList()),
//                statsAggregator.getMeasures().stream().map(statsAggregator::getLastDataPoint).collect(Collectors.toList()),
//                statsAggregator.getMeasures().stream().map(statsAggregator::getMinValue).collect(Collectors.toList()),
//                statsAggregator.getMeasures().stream().map(statsAggregator::getMaxValue).collect(Collectors.toList()));
//        groupTimestamp = currentGroupTimestamp;
//        i++;
//        return aggregatedDataPoint;
        return null;
    }
}
