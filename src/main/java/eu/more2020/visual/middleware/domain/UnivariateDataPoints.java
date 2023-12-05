package eu.more2020.visual.middleware.domain;

/**
 * Represents a sequence of uni-variate data point that can be traversed in time-ascending order.
 */
public interface UnivariateDataPoints extends Iterable<UnivariateDataPoint>, TimeInterval  {

    /*
        The measure of the underlying datapoints
     */
    int getMeasure();
}
