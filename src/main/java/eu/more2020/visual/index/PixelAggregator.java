package eu.more2020.visual.index;

import eu.more2020.visual.domain.*;
import eu.more2020.visual.util.DateTimeUtil;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PixelAggregator implements Iterator<PixelAggregatedDataPoint>, PixelAggregatedDataPoint {

    protected final MultiSpanIterator multiSpanIterator;
    protected final AggregateInterval m4Interval;
    protected final ViewPort viewPort;
    private PixelStatsAggregator statsAggregator;
    private StatsAggregator globalStatsAggregator;

    /**
     * The start date time value of the current pixel.
     */
    private ZonedDateTime currentPixel;
    private ZonedDateTime currentSubPixel;

    /**
     * The start date time value of the next pixel.
     */
    private ZonedDateTime nextPixel;

    private PixelAggregatedDataPoint aggregatedDataPoint = null;

    private AggregateInterval subInterval;

    private boolean hasRemainder = false;

    private final long from;
    private final long to;
    private final List<Integer> measures;
    private final ViewPort viewport;
    private final TotalErrorEvaluator totalErrorEvaluator;

    private Iterator<PixelAggregatedDataPoint> pixelAggregatedDataPointIterator;


    public PixelAggregator(MultiSpanIterator multiSpanIterator, long from, long to, List<Integer> measures,
                              AggregateInterval m4Interval, ViewPort viewport) {
        this.multiSpanIterator = multiSpanIterator;
        this.m4Interval = m4Interval;
        this.viewPort = viewport;
        this.from = from;
        this.to = to;
        this.measures = measures;
        this.viewport = viewport;
        calculateStats(multiSpanIterator);
        this.statsAggregator = new PixelStatsAggregator(globalStatsAggregator, measures, viewport);
        this.totalErrorEvaluator = new TotalErrorEvaluator(statsAggregator, measures);
    }

    private void calculateStats(MultiSpanIterator multiSpanIterator) {
        this.globalStatsAggregator = new StatsAggregator(measures);
        List<PixelAggregatedDataPoint> aggregatedDataPoints = new ArrayList<>();
        SubPixelAggregator subPixelAggregator = new SubPixelAggregator(multiSpanIterator, from , to, measures, m4Interval, viewport);
        while (subPixelAggregator.hasNext()) {
            PixelAggregatedDataPoint next = subPixelAggregator.next().persist();
                aggregatedDataPoints.add(next);
                globalStatsAggregator.accept(next);
        }
        pixelAggregatedDataPointIterator = aggregatedDataPoints.iterator();
    }

    @Override
    public boolean hasNext() {
        return pixelAggregatedDataPointIterator.hasNext();
    }

    /**
     * Collects all pixel aggregated datapoints that belong to the current pixel column.
     * If there is a partial interval it handles its datapoints by opening it up and only keeping those that belong in this pixel column.
     * @return This stats aggregator, based on current pixel.
     */
    @Override
    public PixelAggregatedDataPoint next() {
        moveToNextPixel();
        while((currentSubPixel
                .isBefore(nextPixel)) && hasNext()) {
            moveToNextSubPixel();
            statsAggregator.accept(aggregatedDataPoint); // add to stats
        }
        hasRemainder = !currentSubPixel.equals(nextPixel); // is a partial overlap
        if(hasRemainder && hasNext()) {
            statsAggregator.accept(aggregatedDataPoint, currentPixel, nextPixel, true);
        }
        totalErrorEvaluator.accept(this);
        return this;
    }

    /**
     * Move to next aggregated data point.
     * Change the sub pixel and its interval to correspond to the level of aggregation of the datapoint.
     */
    private void moveToNextSubPixel() {
        aggregatedDataPoint = pixelAggregatedDataPointIterator.next(); // go to next datapoint
        subInterval = aggregatedDataPoint.getInterval();
        currentSubPixel = DateTimeUtil.getIntervalStart(aggregatedDataPoint.getTimestamp(), subInterval, ZoneId.of("UTC")); // go to next sub pixel
    }

    /**
     * 1. Initializes the Aggregator.
     *  a) Read first sub pixel column.
     *  b) Read until the start of the query interval.
     *  c) Initialize pixel columns and add the first datapoint to the aggregator.
     *
     *2. Changes the pixel column.
     *  If there is a remainder from the previous pixel column add it to the aggregator.
     */
    private void moveToNextPixel() {
        statsAggregator.clear();
        if (currentPixel == null) {
            moveToNextSubPixel();
            currentPixel = DateTimeUtil.getIntervalStart(aggregatedDataPoint.getTimestamp(), m4Interval, ZoneId.of("UTC"));
            nextPixel = currentPixel.plus(m4Interval.getInterval(), m4Interval.getChronoUnit());
            statsAggregator.accept(aggregatedDataPoint);
        } else {
            currentPixel = currentPixel.plus(m4Interval.getInterval(), m4Interval.getChronoUnit());
            nextPixel = nextPixel.plus(m4Interval.getInterval(), m4Interval.getChronoUnit());
            if(hasRemainder) {
                statsAggregator.accept(aggregatedDataPoint, currentPixel, nextPixel, false);
                hasRemainder = false;
            }
        }
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

        public ImmutablePixelDatapoint(PixelAggregator subPixelAggregator){
            this(subPixelAggregator.getStats(), subPixelAggregator.getInterval(),
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
            return this;
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
