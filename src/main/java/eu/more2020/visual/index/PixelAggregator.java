package eu.more2020.visual.index;

import eu.more2020.visual.domain.*;
import eu.more2020.visual.util.DateTimeUtil;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

public class PixelAggregator implements Iterator<AggregatedDataPoint>, AggregatedDataPoint {

    private Iterator<PixelAggregatedDataPoint> pixelAggregatedDataPointIterator;

    private PixelAggregatedDataPoint pixelAggregatedDataPoint;
    private final StatsAggregator statsAggregator;
    private final List<Integer> measures;
    private final AggregateInterval m4Interval;
    private final ViewPort viewport;
    private final Queue<PixelAggregatedDataPoint> processedDataPoints;
    private final TotalErrorEvaluator totalErrorEvaluator;

    private ZonedDateTime currentPixel;
    private ZonedDateTime nextPixel;
    private boolean finishedIt = false;

    public PixelAggregator(MultiSpanIterator multiSpanIterator, List<Integer> measures,
                           AggregateInterval m4Interval, ViewPort viewport) {
        this.measures = measures;
        this.m4Interval = m4Interval;
        this.viewport = viewport;
        this.statsAggregator = new StatsAggregator(measures);
        this.processedDataPoints = new LinkedList<>();
        this.totalErrorEvaluator = new TotalErrorEvaluator(statsAggregator, measures, viewport, m4Interval);
        calculateStats(multiSpanIterator);
    }

    private void calculateStats(MultiSpanIterator multiSpanIterator) {
        List<PixelAggregatedDataPoint> aggregatedDataPoints = new ArrayList<>();
        SubPixelAggregator subPixelAggregator = new SubPixelAggregator(multiSpanIterator, measures, m4Interval, viewport);
        while (subPixelAggregator.hasNext()) {
            PixelAggregatedDataPoint next = subPixelAggregator.next().persist();
            System.out.println(next.getStats().getMinValue(4));
            aggregatedDataPoints.add(next);
            statsAggregator.accept(next);
        }
        pixelAggregatedDataPointIterator = aggregatedDataPoints.iterator();
    }

    @Override
    public boolean hasNext() {
        return (!processedDataPoints.isEmpty() || pixelAggregatedDataPointIterator.hasNext());
    }

    public PixelAggregatedDataPoint getNext() {
        if(!processedDataPoints.isEmpty()) return processedDataPoints.poll();
        if(pixelAggregatedDataPointIterator.hasNext()) return pixelAggregatedDataPointIterator.next();
        throw new NoSuchElementException();
    }

    @Override
    public AggregatedDataPoint next() {
        // get current pixel data
        moveToNextPixel();
        while(pixelAggregatedDataPoint.startsBefore(nextPixel) && pixelAggregatedDataPointIterator.hasNext()) {
            processedDataPoints.add(pixelAggregatedDataPoint);
            totalErrorEvaluator.accept(pixelAggregatedDataPoint);
            pixelAggregatedDataPoint = pixelAggregatedDataPointIterator.next();
        }
        // add last point if finished
        if(!pixelAggregatedDataPointIterator.hasNext() && !finishedIt){
            processedDataPoints.add(pixelAggregatedDataPoint);
            finishedIt = true;
        }
        totalErrorEvaluator.update();
        System.out.println(totalErrorEvaluator.getError(0));
        return getNext();
    }

    private void moveToNextPixel() {
        if (currentPixel == null) {
            pixelAggregatedDataPoint = pixelAggregatedDataPointIterator.next();
            currentPixel = DateTimeUtil.getIntervalStart(pixelAggregatedDataPoint.getTimestamp(), m4Interval, ZoneId.of("UTC"));
            nextPixel = currentPixel.plus(m4Interval.getInterval(), m4Interval.getChronoUnit());
            totalErrorEvaluator.initialize(currentPixel);
        } else {
            // move pixel only if sub pixel aggregation is not complete
            if(!pixelAggregatedDataPoint.startsBefore(nextPixel) && pixelAggregatedDataPointIterator.hasNext()) {
                currentPixel = currentPixel.plus(m4Interval.getInterval(), m4Interval.getChronoUnit());
                nextPixel = nextPixel.plus(m4Interval.getInterval(), m4Interval.getChronoUnit());
                totalErrorEvaluator.add(nextPixel);
            }
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
