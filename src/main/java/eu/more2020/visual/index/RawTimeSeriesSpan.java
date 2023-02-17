package eu.more2020.visual.index;

import eu.more2020.visual.domain.AggregateInterval;
import eu.more2020.visual.domain.DataPoint;
import eu.more2020.visual.domain.DataPoints;
import eu.more2020.visual.domain.TimeInterval;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;

public class RawTimeSeriesSpan implements DataPoints, TimeInterval {

    protected DataPoints dataPoints;
    private ZoneId zoneId;

    public RawTimeSeriesSpan() {}

    public void build(DataPoints dataPoints, ZoneId zoneId) {
        this.dataPoints = dataPoints;
        this.zoneId = zoneId;
    }

    @Override
    public List<Integer> getMeasures() {
        return null;
    }

    @Override
    public long getFrom() {
        return dataPoints.getFrom();
    }

    @Override
    public long getTo() {
        return dataPoints.getTo();
    }

    @Override
    public String getFromDate() {
        return Instant.ofEpochMilli(getFrom()).atZone(zoneId).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    @Override
    public String getToDate() {
        return Instant.ofEpochMilli(getTo()).atZone(zoneId).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    @NotNull
    @Override
    public Iterator<DataPoint> iterator() {
        return null;
    }
}
