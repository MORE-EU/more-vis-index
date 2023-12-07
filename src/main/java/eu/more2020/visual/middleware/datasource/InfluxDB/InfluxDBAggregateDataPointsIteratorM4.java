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
import java.util.Map;
import java.util.NoSuchElementException;

public class InfluxDBAggregateDataPointsIteratorM4 implements Iterator<AggregatedDataPoint> {

    private static final Logger LOG = LoggerFactory.getLogger(DataSource.class);

    private int current;
    private long groupTimestamp, currentGroupTimestamp;

    private final Integer numberOfTables;
    private int currentTable;
    private int currentSize;
    private List<FluxRecord> currentRecords;
    private final List<FluxTable> tables;

    public InfluxDBAggregateDataPointsIteratorM4(List<FluxTable> tables) {
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
       Collect every 4 datapoints and create an aggregated datapoint.
       Group timestamp represents the start time of the aggregated datapoint.
       Current group timestamp is the current timestamp of the group and is set to the end of the aggregated datapoint.
    * */
    private AggregatedDataPoint createAggregatedDataPoint() {
        StatsAggregator statsAggregator = new StatsAggregator();
        String measure = "";
        for(int i = 0; i < 4; i ++){
            FluxRecord record = currentRecords.get(current);
            if(record.getValue() != null) { // check for empty value
                double value = (double) record.getValue();
                long timestamp = ((Instant) record.getValues().get("_time")).toEpochMilli();
                measure = record.getField();
                statsAggregator.accept(new ImmutableDataPoint(timestamp, value));
            }
            currentGroupTimestamp = ((Instant) currentRecords.get(current).getValues().get("_stop")).toEpochMilli();
            current++;
        }
        AggregatedDataPoint aggregatedDataPoint = new ImmutableAggregatedDataPoint(groupTimestamp, currentGroupTimestamp, statsAggregator);
        LOG.debug("Created aggregate Datapoint {} - {} with first: {}, last: {}, min: {} and max: {} ",
                groupTimestamp, currentGroupTimestamp, aggregatedDataPoint.getStats().getFirstValue(),
                aggregatedDataPoint.getStats().getLastValue(),aggregatedDataPoint.getStats().getMinValue(), aggregatedDataPoint.getStats().getMaxValue());
        groupTimestamp = currentGroupTimestamp;
        return aggregatedDataPoint;
    }
}
