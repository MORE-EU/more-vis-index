package eu.more2020.visual.middleware.cache;
import eu.more2020.visual.middleware.domain.DataPoints;
import eu.more2020.visual.middleware.domain.TimeInterval;
import eu.more2020.visual.middleware.domain.UnivariateDataPoints;

import java.util.Iterator;
import java.util.List;
/**
 * Represents an interval in time for a single measure. To be stored in an interval tree.
 */
public interface TimeSeriesSpan extends TimeInterval, UnivariateDataPoints {
    /*
        Iterator for the objects in this time series span.
     */
    Iterator iterator(long from, long to);

    /**
     * The number of time series points fetched form the database behind every data point included in this time series span.
     * When the time series span corresponds to raw, non aggregated data, this number is 1.
     */
    int[] getCounts();

    /*
        Calculate the memory size of this span.
     */
    long calculateDeepMemorySize();

    /*
        Measure corresponding to this time series span.
     */
    int getMeasure();

    /*
        Return the aggregate Interval of this span. For raw it is equal to -1.
     */
    long getAggregateInterval();
}
