package eu.more2020.visual.index;

import eu.more2020.visual.domain.*;
import eu.more2020.visual.util.DateTimeUtil;
import org.xbill.DNS.Zone;

import java.time.ZoneId;
import java.time.ZonedDateTime;
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
    protected ZonedDateTime currentSubPixel;


    /**
     * The start date time value of the next pixel.
     */
    protected ZonedDateTime nextPixel;

    protected AggregatedDataPoint aggregatedDatapoint = null;

    protected AggregateInterval subInterval;

    protected boolean hasRemainder = false;


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
        if(hasRemainder) return remainder();
        moveToNextPixel();
        // While nextSubPixel is before the nextPixel
        while((currentSubPixel
                .plus(2 * subInterval.getInterval(), subInterval.getChronoUnit())
                .isBefore(nextPixel) ||
                currentSubPixel
                .plus(2 * subInterval.getInterval(), subInterval.getChronoUnit())
                .equals(nextPixel)) && hasNext()) {
            aggregatedDatapoint = (AggregatedDataPoint) multiSpanIterator.next();
            currentSubPixel = currentSubPixel.plus(subInterval.getInterval(), subInterval.getChronoUnit());
            statsAggregator.accept(aggregatedDatapoint);
            subInterval = ((TimeSeriesSpan) multiSpanIterator.getCurrentIterable()).getAggregateInterval();
        }
        hasRemainder = !currentSubPixel.plus(subInterval.getInterval(), subInterval.getChronoUnit()).equals(nextPixel);
        return this;
    }

    protected AggregatedDataPoint remainder() {
        hasRemainder = false;
        aggregatedDatapoint = (AggregatedDataPoint) multiSpanIterator.next();
        currentSubPixel = currentSubPixel.plus(subInterval.getInterval(), subInterval.getChronoUnit());
        return new OverlappingDatapoint(aggregatedDatapoint, currentSubPixel);
    }

    protected void moveToNextPixel() {
        subInterval = ((TimeSeriesSpan) multiSpanIterator.getCurrentIterable()).getAggregateInterval();
        statsAggregator.clear();
        if (currentPixel == null) {
            aggregatedDatapoint = (AggregatedDataPoint) multiSpanIterator.next();
            currentSubPixel = DateTimeUtil.getIntervalStart(aggregatedDatapoint.getTimestamp(), subInterval, ZoneId.of("UTC"));
            currentPixel = DateTimeUtil.getIntervalStart(aggregatedDatapoint.getTimestamp(), m4Interval, ZoneId.of("UTC"));
            nextPixel = currentPixel.plus(m4Interval.getInterval(), m4Interval.getChronoUnit());
            statsAggregator.accept(aggregatedDatapoint);
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
        return currentPixel.toInstant().toEpochMilli();
    }

    @Override
    public double[] getValues() {
        throw new UnsupportedOperationException();
    }

    private static class OverlappingDatapoint implements AggregatedDataPoint {

        private final ZonedDateTime subPixel;
        private final AggregatedDataPoint aggregatedDataPoint;

        public OverlappingDatapoint(AggregatedDataPoint overlappingDatapoint,
                                    ZonedDateTime overlappingSubPixel) {
            aggregatedDataPoint = overlappingDatapoint;
            subPixel = overlappingSubPixel;
        }

        @Override
        public int getCount() {
            return aggregatedDataPoint.getCount();
        }

        @Override
        public Stats getStats() {
            return aggregatedDataPoint.getStats();
        }

        @Override
        public long getTimestamp() {
            return subPixel.toInstant().toEpochMilli();
        }

        @Override
        public double[] getValues() {
            throw new UnsupportedOperationException();
        }
    }
}
