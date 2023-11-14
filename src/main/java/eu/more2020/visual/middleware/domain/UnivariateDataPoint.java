package eu.more2020.visual.middleware.domain;


import eu.more2020.visual.middleware.util.DateTimeUtil;

/**
 * Represents a single univariate data point with a single value and a timestamp.
 */
public class UnivariateDataPoint {

    private final long timestamp;

    private final Double value;

    public UnivariateDataPoint(final long timestamp, final Double value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    /**
     * Returns the timestamp(epoch time in milliseconds) of this data point.
     */
    public long getTimestamp() {
        return timestamp;
    }


    /**
     * Returns a single measure value for the {@code timestamp)
     */
    public Double getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "{" + timestamp + ", " + DateTimeUtil.format(timestamp) +
                ", " + value +
                '}';
    }
}
