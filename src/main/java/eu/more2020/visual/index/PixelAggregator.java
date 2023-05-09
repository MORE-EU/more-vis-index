package eu.more2020.visual.index;

import eu.more2020.visual.domain.*;
import eu.more2020.visual.util.DateTimeUtil;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

public class PixelAggregator implements Iterator<PixelAggregatedDataPoint>, AggregatedDataPoint {

    private Iterator<PixelAggregatedDataPoint> pixelAggregatedDataPointIterator;

    private final StatsAggregator statsAggregator;
    private final List<Integer> measures;
    private final AggregateInterval m4Interval;
    private final ViewPort viewport;
    private final TotalErrorEvaluator totalErrorEvaluator;

    private PixelAggregatedDataPoint pixelAggregatedDataPoint;

    public PixelAggregator(MultiSpanIterator multiSpanIterator, List<Integer> measures,
                           AggregateInterval m4Interval, ViewPort viewport) {
        this.measures = measures;
        this.m4Interval = m4Interval;
        this.viewport = viewport;
        this.statsAggregator = new StatsAggregator(measures);
        this.totalErrorEvaluator = new TotalErrorEvaluator(statsAggregator, measures, viewport, m4Interval);
        calculateStats(multiSpanIterator);
    }

    private void calculateStats(MultiSpanIterator multiSpanIterator) {
        List<PixelAggregatedDataPoint> aggregatedDataPoints = new ArrayList<>();
        SubPixelAggregator subPixelAggregator = new SubPixelAggregator(multiSpanIterator, measures, m4Interval, viewport);
        while (subPixelAggregator.hasNext()) {
            PixelAggregatedDataPoint next = subPixelAggregator.next().persist();
            if(next.getCount() != 0) {
                aggregatedDataPoints.add(next);
                statsAggregator.accept(next);
            }
        }
        pixelAggregatedDataPointIterator = aggregatedDataPoints.iterator();
    }

    @Override
    public boolean hasNext() {
        return pixelAggregatedDataPointIterator.hasNext();
    }


    @Override
    public PixelAggregatedDataPoint next() {
        pixelAggregatedDataPoint = pixelAggregatedDataPointIterator.next();
        return pixelAggregatedDataPoint;
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

    public double getError(int measure){
        return totalErrorEvaluator.getError(measure);
    }


}
