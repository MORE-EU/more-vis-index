package eu.more2020.visual.middleware.domain;


import eu.more2020.visual.middleware.util.DateTimeUtil;

/**
 * Represents an immutable single univariate data point with a single value and a timestamp.
 */
public class ImmutableUnivariateDataPoint implements UnivariateDataPoint {

    private final long timestamp;

    private final double value;

    public ImmutableUnivariateDataPoint(final long timestamp, final double value) {
        this.timestamp = timestamp;
        this.value = value;
    }


    public long getTimestamp() {
        return timestamp;
    }

    public double getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "{" + timestamp + ", " + DateTimeUtil.format(timestamp) +
                ", " + value +
                '}';
    }
}
