package eu.more2020.visual.index;

import eu.more2020.visual.domain.*;
import eu.more2020.visual.util.DateTimeUtil;
import org.apache.commons.lang3.SerializationUtils;
import org.xbill.DNS.Zone;

import java.io.Serializable;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.List;

public class SubPixelAggregator implements Iterator<PixelAggregatedDataPoint>, PixelAggregatedDataPoint {

    protected final MultiSpanIterator multiSpanIterator;
    protected final AggregateInterval m4Interval;
    protected final ViewPort viewPort;
    private final long from;
    private final long to;

    private boolean first = false;

    private final SubPixelStatsAggregator statsAggregator;

    /**
     * The start date time value of the current pixel.
     */
    private ZonedDateTime currentPixel;

    private ZonedDateTime currentSubPixel;

    /**
     * The start date time value of the next pixel.
     */
    private ZonedDateTime nextPixel;

    private AggregatedDataPoint aggregatedDataPoint = null;

    private AggregateInterval subInterval;


    public SubPixelAggregator(MultiSpanIterator multiSpanIterator, long from, long to,
                              List<Integer> measures, AggregateInterval m4Interval, ViewPort viewport) {
        this.multiSpanIterator = multiSpanIterator;
        this.from = from;
        this.to = to;
        this.m4Interval = m4Interval;
        this.viewPort = viewport;
        this.statsAggregator = new SubPixelStatsAggregator(measures);
        initialize();
    }

    @Override
    public boolean hasNext(){
        return multiSpanIterator.hasNext();
    }

    /**
     * Collects all sub-pixel aggregated datapoints that belong to the current pixel column.
     * If there is a partial interval it keeps it and returns it whole in the next iteration.
     */
    @Override
    public PixelAggregatedDataPoint next() {
        if(first) {
            first = false;
            return this;
        }
        statsAggregator.clear();
        moveToNextSubPixel();
        statsAggregator.accept(aggregatedDataPoint);
        return this;
    }

    /**
     * Move to next aggregated data point.
     * Change the sub pixel and its interval to correspond to the level of aggregation of the datapoint.
     */
    void moveToNextSubPixel() {
        aggregatedDataPoint = (AggregatedDataPoint) multiSpanIterator.next(); // go to next datapoint
        subInterval = ((TimeSeriesSpan) multiSpanIterator.getCurrentIterable()).getAggregateInterval();
        currentSubPixel = DateTimeUtil.getIntervalStart(aggregatedDataPoint.getTimestamp(), subInterval, ZoneId.of("UTC")); // go to next sub pixel
    }

    private void initialize() {
        if (currentPixel == null) {
            moveToNextSubPixel();
            while (hasNext() &&
                    (aggregatedDataPoint.getTimestamp() < DateTimeUtil.getIntervalStart(from, subInterval, ZoneId.of("UTC")).toInstant().toEpochMilli()))
                moveToNextSubPixel();
            currentPixel = DateTimeUtil.getIntervalStart(aggregatedDataPoint.getTimestamp(), m4Interval, ZoneId.of("UTC"));
            nextPixel = DateTimeUtil.getIntervalEnd(aggregatedDataPoint.getTimestamp(), m4Interval, ZoneId.of("UTC"));
            statsAggregator.accept(aggregatedDataPoint);
            first = true;
        }
    }

    @Override
    public int getCount() {
        return statsAggregator.getCount();
    }

    @Override
    public StatsAggregator getStats() {
        return statsAggregator;
    }

    @Override
    public long getTimestamp() {
        return currentSubPixel.toInstant().toEpochMilli();
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
    public ZonedDateTime getNextPixel() { return nextPixel; }

    @Override
    public AggregateInterval getInterval() {
        return subInterval;
    }

    public PixelAggregatedDataPoint persist() {
        return new ImmutableSubPixelDatapoint(this);
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

    public ZonedDateTime getSubPixel() {return currentSubPixel;}

    private static class ImmutableSubPixelDatapoint implements PixelAggregatedDataPoint {

        private final ZonedDateTime subPixel;
        private final ZonedDateTime currentPixel;
        private final ZonedDateTime nextPixel;

        private final AggregateInterval interval;

        private final SubPixelStatsAggregator stats;

        public ImmutableSubPixelDatapoint(SubPixelAggregator subPixelAggregator){
            this(subPixelAggregator.getStats(), subPixelAggregator.getInterval(), subPixelAggregator.getSubPixel(),
                    subPixelAggregator.getPixel(), subPixelAggregator.getNextPixel());
        }

        private ImmutableSubPixelDatapoint(Stats stats, AggregateInterval subInterval,
                                  ZonedDateTime subPixel, ZonedDateTime currentPixel, ZonedDateTime nextPixel) {
            this.stats = ((SubPixelStatsAggregator) stats).clone();
            this.subPixel = subPixel;
            this.currentPixel = currentPixel;
            this.nextPixel = nextPixel;
            this.interval = subInterval;
        }

        @Override
        public int getCount() {
            return stats.getCount();
        }

        @Override
        public Stats getStats() {
            return stats;
        }

        @Override
        public long getTimestamp() {
            return subPixel.toInstant().toEpochMilli();
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
            return this.subPixel.isBefore(zonedDateTime);
        }

        @Override
        public boolean endsBefore(ZonedDateTime zonedDateTime) {
            return this.subPixel.plus(this.interval.getInterval(), this.interval.getChronoUnit()).isBefore(zonedDateTime);
        }

        @Override
        public boolean endsAfter(ZonedDateTime zonedDateTime) {
            return this.subPixel.plus(this.interval.getInterval(), this.interval.getChronoUnit()).isAfter(zonedDateTime);
        }

        public ZonedDateTime getSubPixel() {
            return subPixel;
        }

        public boolean isOverlapping() {
            long currentSubPixelEnd = subPixel
                    .plus(interval.getInterval(), interval.getChronoUnit())
                    .toInstant().toEpochMilli();
            long nextPixelStart = nextPixel.toInstant().toEpochMilli();
            return nextPixelStart < currentSubPixelEnd;
        }
    }
}