package eu.more2020.visual.domain;


import eu.more2020.visual.util.DateTimeUtil;

import java.util.Arrays;

/**
 * Represents a single, immutable multi-measure data point with a number of values and a timestamp.
 */
public class ImmutableAggregatedDataPoint implements AggregatedDataPoint {

    /**
     * The timestamp of the first data point.
     */
    private final long timestamp;

    /**
     * An array of double values, corresponding to a set of measures.
     * The mapping of each value to a measure is handled elsewhere,
     * depending on the specific case.
     */
    private final Stats stats;


    /**
     * Creates a new data point with a timestamp and an array of values.
     *
     * @param timestamp A timestamp.
     * @param stats    The data point statistics.
     */
    public ImmutableAggregatedDataPoint(final long timestamp, final Stats stats) {
        this.timestamp = timestamp;
        this.stats = stats;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public double[] getValues() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getCount() {
        return stats.getCount();
    }

    @Override
    public Stats getStats() {
        return stats;
    }

}
