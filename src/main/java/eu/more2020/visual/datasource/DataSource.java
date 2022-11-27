package eu.more2020.visual.datasource;

import eu.more2020.visual.domain.DataPoints;
import eu.more2020.visual.domain.TimeRange;

import java.util.List;

/**
 * Represents a time series data source
 */
public interface DataSource {


    /**
     * Returns a {@link DataPoints} instance to access the data points in the time series, that
     * have a timestamp greater than or equal to the startTimestamp,
     * and less than or equal to the endTimestamp.
     *
     * @param timeRange The time range to fetch
     * @param measures       The measure values to include in every data point
     */
    public DataPoints getDataPoints(TimeRange timeRange, List<Integer> measures);

    /**
     * Returns a {@link DataPoints} instance to access all the data points in the time series.
     *
     * @param measures The measure values to include in every data point
     */
    public DataPoints getAllDataPoints(List<Integer> measures);

}
