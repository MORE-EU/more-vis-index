package eu.more2020.visual.domain;


import eu.more2020.visual.util.DateTimeUtil;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;

/**
 * Represents a single, immutable multi-measure data point with a number of values and a timestamp.
 */
public class ImmutableDataPoint implements DataPoint {

    /**
     * The timestamp of the data point.
     */
    private final long timestamp;

    /**
     * An array of double values, corresponding to a set of measures.
     * The mapping of each value to a measure is handled elsewhere,
     * depending on the specific case.
     */
    private final double[] values;


    /**
     * Creates a new data point with a timestamp and an array of values.
     *
     * @param timestamp A timestamp.
     * @param values    The data point values.
     */
    public ImmutableDataPoint(final long timestamp, final double[] values) {
        this.timestamp = timestamp;
        this.values = values;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public double[] getValues() {
        return values;
    }

    @Override
    public String toString() {
        return "DataPoint{" +
                "timestamp=" + DateTimeUtil.format(timestamp)  +
                ", values=" + Arrays.toString(values) +
                '}';
    }
}
