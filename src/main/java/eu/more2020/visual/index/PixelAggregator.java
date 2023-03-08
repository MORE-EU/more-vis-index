package eu.more2020.visual.index;

import eu.more2020.visual.domain.*;
import eu.more2020.visual.util.DateTimeUtil;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PixelAggregator implements Iterator<AggregatedDataPoint>, AggregatedDataPoint {

    private Iterator<PixelAggregatedDataPoint> pixelAggregatedDataPointIterator;

    private PixelAggregatedDataPoint pixelAggregatedDataPoint;
    private final StatsAggregator statsAggregator;
    private final List<Integer> measures;
    private final AggregateInterval m4Interval;
    private final ViewPort viewport;
    private final List<PixelColumnErrorAggregator> errors = new ArrayList<>();

    private int currentPixelColumn;
    private PixelColumnErrorAggregator pixelColumnErrorAggregator;

    private ZonedDateTime currentPixel;
    private ZonedDateTime nextPixel;


    public PixelAggregator(MultiSpanIterator multiSpanIterator, List<Integer> measures,
                           AggregateInterval m4Interval, ViewPort viewport) {
        this.measures = measures;
        this.m4Interval = m4Interval;
        this.viewport = viewport;
        this.statsAggregator = new StatsAggregator(measures);
        this.pixelColumnErrorAggregator = new PixelColumnErrorAggregator(measures);
        calculateStats(multiSpanIterator);
    }

    private void calculateStats(MultiSpanIterator multiSpanIterator) {
        List<PixelAggregatedDataPoint> aggregatedDataPoints = new ArrayList<>();
        SubPixelAggregator subPixelAggregator = new SubPixelAggregator(multiSpanIterator, measures, m4Interval, viewport);
        while (subPixelAggregator.hasNext()) {
            PixelAggregatedDataPoint next = subPixelAggregator.next().persist();
            aggregatedDataPoints.add(next);
            statsAggregator.accept(next);
        }
        pixelAggregatedDataPointIterator = aggregatedDataPoints.iterator();
    }

    @Override
    public boolean hasNext() {
        return pixelAggregatedDataPointIterator.hasNext();
    }


    @Override
    public AggregatedDataPoint next() {
        moveToNextPixel();
        return pixelAggregatedDataPoint;
    }

    private void moveToNextPixel() {
        if (currentPixel == null) {
            pixelAggregatedDataPoint = pixelAggregatedDataPointIterator.next();
            currentPixel = DateTimeUtil.getIntervalStart(pixelAggregatedDataPoint.getTimestamp(), m4Interval, ZoneId.of("UTC"));
            nextPixel = currentPixel.plus(m4Interval.getInterval(), m4Interval.getChronoUnit());
            pixelColumnErrorAggregator.accept(pixelAggregatedDataPoint);
        } else {
            currentPixel = currentPixel.plus(m4Interval.getInterval(), m4Interval.getChronoUnit());
            nextPixel = nextPixel.plus(m4Interval.getInterval(), m4Interval.getChronoUnit());
        }
    }

    @Override
    public int getCount() {
        return pixelAggregatedDataPoint.getCount();
    }

    @Override
    public Stats getStats() {
        return pixelAggregatedDataPoint.getStats();
    }

    @Override
    public long getTimestamp() {
        return pixelAggregatedDataPoint.getTimestamp();
    }

    @Override
    public double[] getValues() {
        throw new UnsupportedOperationException();
    }


}
