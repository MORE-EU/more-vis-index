package eu.more2020.visual.middleware.domain;

/**
 * Represents a single univariate data point with a single value and a timestamp.
 */
public interface DataPoint {
    /**
     * Returns the timestamp(epoch time in milliseconds) of this data point.
     */
    long getTimestamp();

    /**
     * Returns a single measure value for the {@code timestamp)
     */
    double getValue();
}
