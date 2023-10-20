package eu.more2020.visual.middleware.cache;

import eu.more2020.visual.middleware.domain.*;
import eu.more2020.visual.middleware.util.DateTimeUtil;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;


/**
 * An iterator that groups source data points in fixed windows of time.
 * The datapoint groups can be accessed as instances of the {@link AggregatedDataPoint} class.
 * The intervals of the windows are calculated so that they are aligned as well as possible to the start of
 * calendar based intervals (e.g. start of hour, day, month, year) according to the specified timezone.
 * Each group is represented by the start timestamp of the corresponding interval,
 * and includes data points with timestamp greater or equal to the start timestamp,
 * and less than the timestamp of the next group.
 * <p>
 * In case there are no data points for a window interval before the end of the source data, then an empty {@link AggregatedDataPoint}
 * is returned, with the corresponding stats entities set to {@code null} and with {@link AggregatedDataPoint#getCount()} returning {@literal 0}.
 * <p>
 * Note that the object returned by this iterator is always the same every time, and its values must be used before calling the method again.
 */
public class TimeAggregator implements Iterator<AggregatedDataPoint>, AggregatedDataPoint {

    /**
     * The data source iterator
     */

    protected final MultiSpanIterator<DataPoint> sourceDataPointsIterator;

    protected final AggregateInterval aggInterval;


    private final StatsAggregator statsAggregator;

    /**
     * The start date time value of the current interval.
     */
    protected ZonedDateTime currentInterval;


    /**
     * The start date time value of the next interval.
     */
    protected ZonedDateTime nextInterval;

    protected DataPoint nextDataPoint = null;


    /**
     * Constructs a {@link TimeAggregator}
     *
     * @param aggInterval      The aggregation interval.
     */
    public TimeAggregator(final MultiSpanIterator<DataPoint> sourceDataPointsIterator,
                          List<Integer> measures, final AggregateInterval aggInterval) {
        this.sourceDataPointsIterator = sourceDataPointsIterator;
        this.aggInterval = aggInterval;
        statsAggregator = new StatsAggregator(measures);
    }


    @Override
    public boolean hasNext() {
        return sourceDataPointsIterator.hasNext();
    }

    /**
     * Returns the next aggregated data point, corresponding to the next window interval.
     * Note that the object returned is the same every time, and its values must be used before calling the method again.
     *
     * @return the next aggregated data point
     * @throws NoSuchElementException if no data points remain.
     */
    @Override
    public AggregatedDataPoint next() {
        if (sourceDataPointsIterator.hasNext()) {
            moveToNextInterval();
            statsAggregator.clear();
            if (nextDataPoint.getTimestamp() >= currentInterval.toInstant()
                    .toEpochMilli() && nextDataPoint.getTimestamp() < nextInterval.toInstant().toEpochMilli()) {
                statsAggregator.accept(nextDataPoint);
                while (sourceDataPointsIterator.hasNext() && (nextDataPoint = sourceDataPointsIterator.next()).getTimestamp() < nextInterval.toInstant()
                        .toEpochMilli()) {
                    statsAggregator.accept(nextDataPoint);
                }
            }
            return this;
        }
        throw new NoSuchElementException("Νo more datapoints.");
    }

    /**
     * Moves to the next window interval
     */
    protected void moveToNextInterval() {
        // if not initialized, initialize first interval, based on first datapoint in the source data points
        if (currentInterval == null) {
            nextDataPoint = sourceDataPointsIterator.next();
            currentInterval = DateTimeUtil.getIntervalStart(nextDataPoint.getTimestamp(), aggInterval, ZoneId.of("UTC"));
            nextInterval = currentInterval.plus(aggInterval.getInterval(), aggInterval.getChronoUnit());
        } else {
            currentInterval = currentInterval.plus(aggInterval.getInterval(), aggInterval.getChronoUnit());
            nextInterval = nextInterval.plus(aggInterval.getInterval(), aggInterval.getChronoUnit());
        }
    }

    // AggregatedDataPoint interface methods

    @Override
    public long getTimestamp() {
        return currentInterval.toInstant().toEpochMilli();
    }

    @Override
    public long getFrom() {
        return currentInterval.toInstant().toEpochMilli();
    }

    @Override
    public long getTo() {
        return nextInterval.toInstant().toEpochMilli();
    }

    @Override
    public double[] getValues() {
        throw new UnsupportedOperationException();
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
    public String toString() {
        return "{timestamp=" + DateTimeUtil.format(getTimestamp()) + ", count=" + getCount() + ", stats=" + getStats() + '}';
    }
}
