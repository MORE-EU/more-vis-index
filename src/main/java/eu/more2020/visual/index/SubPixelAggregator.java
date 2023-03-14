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
    private StatsAggregator statsAggregator;

    private final List<Integer> measures;

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

    public SubPixelAggregator(MultiSpanIterator multiSpanIterator, List<Integer> measures,
                              AggregateInterval m4Interval, ViewPort viewport) {
        this.multiSpanIterator = multiSpanIterator;
        this.m4Interval = m4Interval;
        this.measures = measures;
        this.viewPort = viewport;
        statsAggregator = new StatsAggregator(measures);
    }


    @Override
    public boolean hasNext() {
        return multiSpanIterator.hasNext();
    }

    @Override
    public PixelAggregatedDataPoint next() {
        if(hasRemainder) return remainder();
        moveToNextPixel();
        // While next sub pixel is to the left of the next pixel
        while((currentSubPixel
                .plus(2 * subInterval.getInterval(), subInterval.getChronoUnit())
                .isBefore(nextPixel) ||
                currentSubPixel
                .plus(2 * subInterval.getInterval(), subInterval.getChronoUnit())
                .equals(nextPixel)) && hasNext()) {
            aggregatedDataPoint = (AggregatedDataPoint) multiSpanIterator.next(); // go to next datapoint
            currentSubPixel = currentSubPixel.plus(subInterval.getInterval(), subInterval.getChronoUnit()); // go to next sub pixel
            statsAggregator.accept(aggregatedDataPoint); // add to stats
            subInterval = ((TimeSeriesSpan) multiSpanIterator.getCurrentIterable()).getAggregateInterval();
        }
        hasRemainder = !currentSubPixel.plus(subInterval.getInterval(), subInterval.getChronoUnit()).equals(nextPixel); // next sub pixel is next pixel
        return this;
    }

    private PixelAggregatedDataPoint remainder() {
        hasRemainder = false;
        aggregatedDataPoint = (AggregatedDataPoint) multiSpanIterator.next();
        statsAggregator.clear();
        statsAggregator.accept(aggregatedDataPoint);
        currentSubPixel = currentSubPixel.plus(subInterval.getInterval(), subInterval.getChronoUnit());
        return new SubPixelDatapoint(this);
    }

    private void moveToNextPixel() {
        subInterval = ((TimeSeriesSpan) multiSpanIterator.getCurrentIterable()).getAggregateInterval();
        statsAggregator.clear();
        if (currentPixel == null) {
            aggregatedDataPoint = (AggregatedDataPoint) multiSpanIterator.next();
            currentSubPixel = DateTimeUtil.getIntervalStart(aggregatedDataPoint.getTimestamp(), subInterval, ZoneId.of("UTC"));
            currentPixel = DateTimeUtil.getIntervalStart(aggregatedDataPoint.getTimestamp(), m4Interval, ZoneId.of("UTC"));
            nextPixel = currentPixel.plus(m4Interval.getInterval(), m4Interval.getChronoUnit());
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
    public Stats getStats() {
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
    public ZonedDateTime getSubPixel() {
        return currentSubPixel;
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
        return new SubPixelDatapoint(this);
    }

    @Override
    public boolean isOverlapping() {
        long currentSubPixelEnd = currentSubPixel
                .plus(subInterval.getInterval(), subInterval.getChronoUnit())
                .toInstant().toEpochMilli();
        long nextPixelStart = nextPixel.toInstant().toEpochMilli();

        return nextPixelStart < currentSubPixelEnd;
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

    private static class SubPixelDatapoint implements PixelAggregatedDataPoint {

        private final ZonedDateTime subPixel;
        private final ZonedDateTime currentPixel;
        private final ZonedDateTime nextPixel;

        private final AggregateInterval interval;

        private final Stats stats;

        public SubPixelDatapoint(SubPixelAggregator subPixelAggregator){
            this(subPixelAggregator.getStats(), subPixelAggregator.getInterval(), subPixelAggregator.getSubPixel(),
                    subPixelAggregator.getPixel(), subPixelAggregator.getNextPixel());
        }

        private SubPixelDatapoint(Stats stats, AggregateInterval subInterval,
                                  ZonedDateTime subPixel, ZonedDateTime currentPixel, ZonedDateTime nextPixel) {
            this.stats = SerializationUtils.clone((StatsAggregator) stats);
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
        public ZonedDateTime getSubPixel() {
            return subPixel;
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
        public boolean isOverlapping() {
            long currentSubPixelEnd = subPixel
                    .plus(interval.getInterval(), interval.getChronoUnit())
                    .toInstant().toEpochMilli();
            long nextPixelStart = nextPixel.toInstant().toEpochMilli();
            return nextPixelStart < currentSubPixelEnd;
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
    }
}
