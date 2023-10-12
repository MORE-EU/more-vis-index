package eu.more2020.visual.index.domain;


/**
 * Represents a single multi-measure data point with a number of values and a timestamp.
 */
public interface DataPoint {

    /**
     * Returns the timestamp(epoch time in milliseconds) of this data point.
     */
    public long getTimestamp();

    /**
     * Returns an array of double values, corresponding to a set of measures.
     * The mapping of each value to a measure is handled elsewhere (e.g. in the {@link DataPoints}
     * that include this data point).
     */
    public double[] getValues();

}
