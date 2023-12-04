package eu.more2020.visual.middleware.datasource.InfluxDB;

import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import eu.more2020.visual.middleware.datasource.DataSource;
import eu.more2020.visual.middleware.domain.DataPoint;
import eu.more2020.visual.middleware.domain.ImmutableDataPoint;
import eu.more2020.visual.middleware.domain.TimeInterval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class InfluxDBDataPointsIterator implements Iterator<DataPoint> {
    private static final Logger LOG = LoggerFactory.getLogger(DataSource.class);

    private final List<List<String>> measures;
    private final Integer numberOfTables;
    private int currentTable;
    private int currentSize;
    private int current;
    private List<TimeInterval> ranges;
    private List<FluxRecord> currentRecords;
    private final List<FluxTable> tables;

    public InfluxDBDataPointsIterator(List<TimeInterval> ranges, List<List<String>> measures, List<FluxTable> tables) {
        this.measures = measures;
        this.ranges = ranges;
        this.currentTable = 0;
        this.tables = tables;
        this.currentRecords = tables.get(currentTable).getRecords();
        this.currentSize = this.currentRecords.size();
        this.numberOfTables = tables.size();
        this.current = 0;
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
                return true;
            }
            else return false;
        }
    }

    @Override
    public DataPoint next() {
        FluxRecord fluxRecord = tables.get(currentTable).getRecords().get(current ++);
        double[] values = new double[measures.get(currentTable).size()];
        for (int i = 0; i < measures.get(currentTable).size(); i++)
            values[i] = (double) fluxRecord.getValues().get(measures.get(currentTable).get(i));
        return new ImmutableDataPoint(Objects.requireNonNull(fluxRecord.getTime()).toEpochMilli(), values);
    }

}
