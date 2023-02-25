package eu.more2020.visual.index;

import eu.more2020.visual.domain.*;
import eu.more2020.visual.util.DateTimeUtil;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PixelAggregator implements Iterator<AggregatedDataPoint>, AggregatedDataPoint {

    protected final MultiSpanIterator multiSpanIterator;
    protected final AggregateInterval m4Interval;
    private final StatsAggregator statsAggregator;

    private final List<Integer> measures;

    /**
     * The start date time value of the current pixel.
     */
    protected ZonedDateTime currentPixel;

    /**
     * The start date time value of the next pixel.
     */
    protected ZonedDateTime nextPixel;
    protected ZonedDateTime overlappingSubPixel = null;


    protected AggregatedDataPoint nextAggregateDatapoint = null;
    protected AggregateInterval subInterval;


    public PixelAggregator(MultiSpanIterator multiSpanIterator, List<Integer> measures,
                           AggregateInterval m4Interval) {
        this.multiSpanIterator = multiSpanIterator;
        this.m4Interval = m4Interval;
        this.measures = measures;
        statsAggregator = new StatsAggregator(measures);
    }

    @Override
    public boolean hasNext() {
        return multiSpanIterator.hasNext();
    }

    @Override
    public AggregatedDataPoint next() {
        moveToNextPixel();
        ZonedDateTime nextSubPixel = DateTimeUtil.getIntervalStart(nextAggregateDatapoint.getTimestamp(), subInterval, ZoneId.of("UTC"));
        statsAggregator.clear();
        while(nextSubPixel.plus(subInterval.getInterval(), subInterval.getChronoUnit()).isBefore(nextPixel) && hasNext()){
            nextAggregateDatapoint = (AggregatedDataPoint) multiSpanIterator.next();
            statsAggregator.accept(nextAggregateDatapoint);
            subInterval = ((TimeSeriesSpan) multiSpanIterator.getCurrentIterable()).getAggregateInterval();
            nextSubPixel = DateTimeUtil.getIntervalStart(nextAggregateDatapoint.getTimestamp(), subInterval, ZoneId.of("UTC"));
        }
        return this;
    }

    protected void moveToNextPixel() {
        if (currentPixel == null) {
            nextAggregateDatapoint = (AggregatedDataPoint) multiSpanIterator.next();
            currentPixel = DateTimeUtil.getIntervalStart(nextAggregateDatapoint.getTimestamp(), m4Interval, ZoneId.of("UTC"));
            nextPixel = currentPixel.plus(m4Interval.getInterval(), m4Interval.getChronoUnit());
        } else {
            currentPixel = currentPixel.plus(m4Interval.getInterval(), m4Interval.getChronoUnit());
            nextPixel = nextPixel.plus(m4Interval.getInterval(), m4Interval.getChronoUnit());
        }
        subInterval = ((TimeSeriesSpan) multiSpanIterator.getCurrentIterable()).getAggregateInterval();
    }

    @Override
    public int getCount() {
        return statsAggregator.getCount();
    }

    @Override
    public Stats getStats() {
        return statsAggregator;
    }

    @Override
    public long getTimestamp() {
        return currentPixel.toInstant().toEpochMilli();
    }

    @Override
    public double[] getValues() {
        throw new UnsupportedOperationException();
    }
}
