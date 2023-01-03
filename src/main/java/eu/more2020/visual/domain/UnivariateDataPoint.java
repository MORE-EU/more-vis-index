package eu.more2020.visual.domain;


/**
 * Represents a single univariate data point with a single value and a timestamp.
 */
public class UnivariateDataPoint {

    private final long timestamp;

    private final double value;

    public UnivariateDataPoint(final long timestamp, final double value) {
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
    public double getValue() {
        return value;
    }

}
