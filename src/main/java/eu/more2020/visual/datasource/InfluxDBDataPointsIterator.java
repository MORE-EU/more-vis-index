package eu.more2020.visual.datasource;

import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import eu.more2020.visual.domain.DataPoint;
import eu.more2020.visual.domain.ImmutableDataPoint;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class InfluxDBDataPointsIterator implements Iterator<DataPoint> {

    private final List<String> measures;
    private final FluxTable table;
    private int current;
    private int size;

    public InfluxDBDataPointsIterator(List<String> measures, FluxTable table) {
        this.measures = measures;
        this.table = table;
        this.current = 0;
        this.size = table.getRecords().size();
    }

    @Override
    public boolean hasNext() {
        return current < size;
    }

    @Override
    public DataPoint next() {
        FluxRecord fluxRecord = table.getRecords().get(current ++);
        double[] values = new double[measures.size()];
        for (int i = 0; i < measures.size(); i++)
            values[i] = (double) fluxRecord.getValues().get(measures.get(i));
        return new ImmutableDataPoint(Objects.requireNonNull(fluxRecord.getTime()).toEpochMilli(), values);
    }

}
