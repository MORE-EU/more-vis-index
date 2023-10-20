/*
package eu.more2020.visual.index;

import eu.more2020.visual.domain.*;
import eu.more2020.visual.util.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

public class PixelAggregator implements Iterator<PixelAggregatedDataPoint>, PixelAggregatedDataPoint {

    private static final Logger LOG = LoggerFactory.getLogger(PixelAggregator.class);

    protected final MultiSpanIterator multiSpanIterator;
    protected final AggregateInterval m4Interval;
    protected final ViewPort viewPort;
    private final PixelStatsAggregator statsAggregator;
    private StatsAggregator globalStatsAggregator;

    */
/**
     * The start date time value of the current pixel.
     *//*

    private ZonedDateTime currentPixel;
    private ZonedDateTime firstPixel;
    private ZonedDateTime lastPixel;

    private ZonedDateTime prevSubPixel;
    private ZonedDateTime currentSubPixel;

    */
/**
     * The start date time value of the next pixel.
     *//*

    private ZonedDateTime nextPixel;

    private PixelAggregatedDataPoint aggregatedDataPoint, prevAggregatedDataPoint = null;

    private AggregateInterval subInterval;


    private final long from;
    private final long to;
    private final List<Integer> measures;
    private final ViewPort viewport;
    private final SubPixelAggregator subPixelAggregator;
    private final TotalErrorEvaluator totalErrorEvaluator;

    private int current = 0;
    private int size = current;

    private final PixelColumn[] pixelColumnData;

    public PixelAggregator(MultiSpanIterator multiSpanIterator, long from, long to, List<Integer> measures,
                              AggregateInterval m4Interval, ViewPort viewport) {
        this.multiSpanIterator = multiSpanIterator;
        this.m4Interval = m4Interval;
        this.viewPort = viewport;
        this.from = from;
        this.to = to;
        this.measures = measures;
        this.viewport = viewport;
        this.pixelColumnData = new PixelColumn[viewport.getWidth() + 1];
        this.subPixelAggregator = new SubPixelAggregator(multiSpanIterator, from , to, measures, m4Interval, viewport);
        initialize();
        this.statsAggregator = new PixelStatsAggregator(globalStatsAggregator, firstPixel, m4Interval, measures, viewport);
        this.totalErrorEvaluator = new TotalErrorEvaluator(statsAggregator, measures, viewport);
    }

    private void initialize() {
        this.globalStatsAggregator = new StatsAggregator(measures);
        current = 0;
        moveToNextSubPixel();
        currentPixel = DateTimeUtil.getIntervalStart(aggregatedDataPoint.getTimestamp(), m4Interval, ZoneId.of("UTC"));
        firstPixel = currentPixel;
        nextPixel = currentPixel.plus(m4Interval.getInterval(), m4Interval.getChronoUnit());
        while (subPixelAggregator.hasNext() && nextPixel.toInstant().toEpochMilli() <= to) {
            List<AggregatedDataPoint> dataPoints, nextDataPoints;
            if(pixelColumnData[current] == null) {
                pixelColumnData[current] = new PixelColumn(currentPixel, nextPixel);
                dataPoints = new ArrayList<>();
            }
            else dataPoints = pixelColumnData[current].getAggregatedDataPoints();
            pixelColumnData[current + 1] = new PixelColumn(nextPixel, nextPixel.plus(m4Interval.getInterval(), m4Interval.getChronoUnit()));
            nextDataPoints = new ArrayList<>();
            while(currentSubPixel.isBefore(nextPixel)
                    && (currentSubPixel.isAfter(currentPixel) || currentSubPixel.equals(currentPixel))
                    && subPixelAggregator.hasNext()) {
                dataPoints.add(aggregatedDataPoint.persist());
                if(aggregatedDataPoint.getCount() > 0) {
                    globalStatsAggregator.accept(aggregatedDataPoint);
                }
                moveToNextSubPixel();
            }
            pixelColumnData[current].setAggregatedDataPoints(dataPoints);
            if(dataPoints.size() == 0) {
                moveToNextPixel();
                continue;
            };
            boolean isPartial = !currentSubPixel.equals(nextPixel) && prevSubPixel.isBefore(nextPixel); // is a partial overlap
            if(isPartial) {
                AggregatedDataPoint persistedAggregatedDatapoint = prevAggregatedDataPoint.persist();
                nextDataPoints.add(persistedAggregatedDatapoint);
                pixelColumnData[current].setPartialRight(true);
                pixelColumnData[current + 1].setPartialLeft(true);
            }
            pixelColumnData[current + 1].setAggregatedDataPoints(nextDataPoints);
            moveToNextPixel();
        }
        current = 0;
    }

    private void moveToNextPixel(){
        size ++;
        current ++;
        currentPixel = currentPixel.plus(m4Interval.getInterval(), m4Interval.getChronoUnit());
        LOG.debug("" + currentPixel.toInstant().toEpochMilli());

        nextPixel = currentPixel.plus(m4Interval.getInterval(), m4Interval.getChronoUnit());
    }

    @Override
    public boolean hasNext() {
        return current < size;
    }

    */
/**
     * Collects all aggregated datapoints that belong to the current pixel column.
     * If there is a partial interval it handles its datapoints by opening it up and only keeping those that belong in this pixel column.
     * @return This stats aggregator, based on current pixel.
     *//*

    @Override
    public PixelAggregatedDataPoint next() {
        statsAggregator.clear();
        aggregateDataPoints(current);
        current ++;
        return this;
    }

    private void aggregateDataPoints(int current){
        PixelColumn pixelColumn = pixelColumnData[current];
        PixelColumn prevPixelColumn = current == 0 ? null : pixelColumnData[current - 1];
        PixelColumn nextPixelColumn = current == size - 1 ? null : pixelColumnData[current + 1];
        List<AggregatedDataPoint> aggregatedDataPoints = pixelColumn.getAggregatedDataPoints();
        for (AggregatedDataPoint aggregatedDataPoint : aggregatedDataPoints){
            if(aggregatedDataPoint.getCount() > 0)
                if ((pixelColumn.getFrom().toInstant().toEpochMilli() > (aggregatedDataPoint.getStats().getMaxTimestamp(2)) ||
                        pixelColumn.getTo().toInstant().toEpochMilli() < (aggregatedDataPoint.getStats().getMaxTimestamp(2))) ||
                        (pixelColumn.getFrom().toInstant().toEpochMilli() > (aggregatedDataPoint.getStats().getMinTimestamp(2)) ||
                                pixelColumn.getTo().toInstant().toEpochMilli() < (aggregatedDataPoint.getStats().getMinTimestamp(2)))) {
//                    System.out.println(pixelColumn.getFrom());
//                    System.out.println(DateTimeUtil.format(aggregatedDataPoint.getTimestamp()));
//                    System.out.println(DateTimeUtil.format(aggregatedDataPoint.getStats().getMinTimestamp(2)));
//                    System.out.println(DateTimeUtil.format(aggregatedDataPoint.getStats().getMaxTimestamp(2)));
//                    System.out.println(pixelColumn.getTo());
//                    System.out.println();
                    continue;
                }
            statsAggregator.accept(aggregatedDataPoint);
        }
        statsAggregator.moveToNextPixel();
        pixelColumn.setStats(statsAggregator.clone());
//        totalErrorEvaluator.accept(this, pixelColumn, prevPixelColumn, nextPixelColumn);
    }

    private void moveToNextSubPixel() {
        prevAggregatedDataPoint = aggregatedDataPoint;
        aggregatedDataPoint = subPixelAggregator.next(); // go to next datapoint
        subInterval = subPixelAggregator.getInterval();
        prevSubPixel = currentSubPixel;
        currentSubPixel = DateTimeUtil.getIntervalStart(aggregatedDataPoint.getTimestamp(), subInterval, ZoneId.of("UTC")); // go to next sub pixel
    }


    public double getError(int m){
        return totalErrorEvaluator.getError(m);
    }

    @Override
    public int getCount() {
        return statsAggregator.getCount();
    }

    @Override
    public PixelStatsAggregator getStats() {
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

    @Override
    public ZonedDateTime getPixel() {
        return currentPixel;
    }

    @Override
    public ZonedDateTime getSubPixel() {
        return currentSubPixel;
    }

    @Override
    public ZonedDateTime getNextPixel() { return nextPixel; }

    @Override
    public AggregateInterval getInterval() {
        return subInterval;
    }

    public PixelAggregatedDataPoint persist() {
        return new ImmutablePixelDatapoint(this);
    }

    @Override
    public boolean startsBefore(ZonedDateTime zonedDateTime) {
        return this.currentSubPixel.isBefore(zonedDateTime);
    }

    @Override
    public boolean endsBefore(ZonedDateTime zonedDateTime) {
        return this.currentSubPixel.plus(this.subInterval.getInterval(), this.subInterval.getChronoUnit()).isBefore(zonedDateTime);
    }

    @Override
    public boolean endsAfter(ZonedDateTime zonedDateTime) {
        return this.currentSubPixel.plus(this.subInterval.getInterval(), this.subInterval.getChronoUnit()).isAfter(zonedDateTime);
    }

    private static class ImmutablePixelDatapoint implements PixelAggregatedDataPoint {

        private final ZonedDateTime currentPixel;
        private final ZonedDateTime nextPixel;

        private final AggregateInterval interval;

        private final PixelStatsAggregator stats;


        public ImmutablePixelDatapoint(PixelAggregatedDataPoint subPixelAggregator){
            this((PixelStatsAggregator) subPixelAggregator.getStats(), subPixelAggregator.getInterval(),
                    subPixelAggregator.getPixel(), subPixelAggregator.getNextPixel());
        }

        private ImmutablePixelDatapoint(PixelStatsAggregator stats, AggregateInterval subInterval,
                                        ZonedDateTime currentPixel, ZonedDateTime nextPixel) {
            this.stats = stats.clone();
            this.currentPixel = currentPixel;
            this.nextPixel = nextPixel;
            this.interval = subInterval;
        }

        @Override
        public int getCount() {
            return stats.getCount();
        }

        @Override
        public PixelStatsAggregator getStats() {
            return stats;
        }

        @Override
        public long getTimestamp() {
            return currentPixel.toInstant().toEpochMilli();
        }

        @Override
        public double[] getValues() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ZonedDateTime getSubPixel() {
            return currentPixel;
        }

        @Override
        public ZonedDateTime getPixel() {
            return currentPixel;
        }

        @Override
        public ZonedDateTime getNextPixel() {
            return nextPixel;
        }

        @Override
        public AggregateInterval getInterval() {
            return interval;
        }

        public PixelAggregatedDataPoint persist() {
            return new ImmutablePixelDatapoint(this);
        }

        @Override
        public boolean startsBefore(ZonedDateTime zonedDateTime) {
            return this.currentPixel.isBefore(zonedDateTime);
        }

        @Override
        public boolean endsBefore(ZonedDateTime zonedDateTime) {
            return this.currentPixel.plus(this.interval.getInterval(), this.interval.getChronoUnit()).isBefore(zonedDateTime);
        }

        @Override
        public boolean endsAfter(ZonedDateTime zonedDateTime) {
            return this.nextPixel.isAfter(zonedDateTime);
        }
    }
}
*/
