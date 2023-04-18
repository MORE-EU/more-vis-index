package eu.more2020.visual.datasource;

import eu.more2020.visual.domain.AggregatedDataPoint;

import java.util.Iterator;

public class InfluxDBAggregateDataPointsIterator implements Iterator<AggregatedDataPoint> {
    
    
    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public AggregatedDataPoint next() {
        return null;
    }
}
