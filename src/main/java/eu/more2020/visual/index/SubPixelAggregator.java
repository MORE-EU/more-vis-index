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

    private boolean hasRemainder = false;

    public SubPixelAggregator(MultiSpanIterator multiSpanIterator, long from, long to,
                              List<Integer> measures, AggregateInterval m4Interval, ViewPort viewport) {
        this.multiSpanIterator = multiSpanIterator;
        this.from = from;
        this.to = to;
        this.m4Interval = m4Interval;
        this.viewPort = viewport;
        statsAggregator = new SubPixelStatsAggregator(measures);
    }

    @Override
    public boolean hasNext() {
        return multiSpanIterator.hasNext();
    }

    /**
     * Collects all sub-pixel aggregated datapoints that belong to the current pixel column.
     * If there is a partial interval it keeps it and returns it whole in the next iteration.
     */
    @Override
    public PixelAggregatedDataPoint next() {
        if(hasRemainder) return remainder();
        moveToNextPixel();
        // While next sub pixel is to the left of the next pixel
        while((currentSubPixel
                .plus( subInterval.getInterval(), subInterval.getChronoUnit())
                .isBefore(nextPixel) ||
                currentSubPixel
                        .plus( subInterval.getInterval(), subInterval.getChronoUnit())
                        .equals(nextPixel)) && hasNext()) {
            moveToNextSubPixel();
            statsAggregator.accept(aggregatedDataPoint); // add to stats
        }
        hasRemainder = currentSubPixel.plus(subInterval.getInterval(), subInterval.getChronoUnit()).isAfter(nextPixel); // next sub pixel is not next pixel
        return this;
    }

    private PixelAggregatedDataPoint remainder() {
        moveToNextSubPixel();
        hasRemainder = false;
        statsAggregator.clear();
        statsAggregator.accept(aggregatedDataPoint);
        return new ImmutableSubPixelDatapoint(this);
    }

    /**
     * Move to next aggregated data point.
     * Change the sub pixel and its interval to correspond to the level of aggregation of the datapoint.
     */
    private void moveToNextSubPixel() {
        aggregatedDataPoint = (AggregatedDataPoint) multiSpanIterator.next(); // go to next datapoint
        subInterval = ((TimeSeriesSpan) multiSpanIterator.getCurrentIterable()).getAggregateInterval();
        currentSubPixel = DateTimeUtil.getIntervalStart(aggregatedDataPoint.getTimestamp(), subInterval, ZoneId.of("UTC")); // go to next sub pixel
    }

    private void moveToNextPixel() {
        statsAggregator.clear();
        if (currentPixel == null) {
            moveToNextSubPixel();
            while (hasNext() && (aggregatedDataPoint.getTimestamp() < DateTimeUtil.getIntervalStart(from, subInterval, ZoneId.of("UTC")).toInstant().toEpochMilli()))
                moveToNextSubPixel();
            currentPixel = DateTimeUtil.getIntervalStart(aggregatedDataPoint.getTimestamp(), m4Interval, ZoneId.of("UTC"));
            nextPixel = DateTimeUtil.getIntervalEnd(aggregatedDataPoint.getTimestamp(), m4Interval, ZoneId.of("UTC"));
            statsAggregator.accept(aggregatedDataPoint);
        } else {
            currentPixel = currentPixel.plus(m4Interval.getInterval(), m4Interval.getChronoUnit());
            nextPixel = nextPixel.plus(m4Interval.getInterval(), m4Interval.getChronoUnit());
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