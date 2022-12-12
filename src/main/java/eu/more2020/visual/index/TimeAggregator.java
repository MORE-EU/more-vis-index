package eu.more2020.visual.index;

import eu.more2020.visual.domain.AggregatedDataPoint;
import eu.more2020.visual.domain.DataPoint;
import eu.more2020.visual.domain.DataPoints;
import eu.more2020.visual.domain.Stats;
import eu.more2020.visual.util.DateTimeUtil;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Iterator;
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
    protected final DataPoints sourceDataPoints;

    protected final Iterator<DataPoint> sourceDataPointsIterator;

    protected final int aggInterval;

    protected final ChronoUnit unit;

    private final Stats stats;

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
     * @param sourceDataPoints The source data points.
     * @param aggInterval      The aggregation interval.
     * @param unit             The unit to use for the aggregation interval.
     */
    public TimeAggregator(final DataPoints sourceDataPoints, final int aggInterval, final ChronoUnit unit) {
        this.sourceDataPoints = sourceDataPoints;
        sourceDataPointsIterator = sourceDataPoints.iterator();
        this.aggInterval = aggInterval;
        this.unit = unit;
        stats = new Stats(sourceDataPoints.getMeasures());
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
            stats.clear();
            if (nextDataPoint.getTimestamp() >= currentInterval.toInstant()
                    .toEpochMilli() && nextDataPoint.getTimestamp() < nextInterval.toInstant().toEpochMilli()) {

                stats.accept(nextDataPoint);
                while (sourceDataPointsIterator.hasNext() && (nextDataPoint = sourceDataPointsIterator.next()).getTimestamp() < nextInterval.toInstant()
                        .toEpochMilli()) {
                    stats.accept(nextDataPoint);
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
            currentInterval = DateTimeUtil.getIntervalStart(nextDataPoint.getTimestamp(), aggInterval, unit, ZoneId.of("UTC"));
            nextInterval = currentInterval.plus(aggInterval, unit);
        } else {
            currentInterval = currentInterval.plus(aggInterval, unit);
            nextInterval = nextInterval.plus(aggInterval, unit);
        }
    }

    // AggregatedDataPoint interface methods

    @Override
    public long getTimestamp() {
        return currentInterval.toInstant().toEpochMilli();
    }

    @Override
    public double[] getValues() {
        return stats.getAverageValues();
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
    public String toString() {
        return "{timestamp=" + DateTimeUtil.format(getTimestamp()) + ", count=" + getCount() + ", stats=" + getStats() + '}';
    }
}