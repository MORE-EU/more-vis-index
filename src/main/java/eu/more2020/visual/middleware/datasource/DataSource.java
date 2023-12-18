package eu.more2020.visual.middleware.datasource;

import eu.more2020.visual.middleware.domain.AggregatedDataPoints;
import eu.more2020.visual.middleware.domain.DataPoints;
import eu.more2020.visual.middleware.domain.Query.QueryMethod;
import eu.more2020.visual.middleware.domain.TimeInterval;

import javax.xml.crypto.Data;
import java.util.List;
import java.util.Map;

/**
 * Represents a time series data source
 */
public interface DataSource {

    /**
     * Returns an {@link AggregatedDataPoints} instance to access the data points in the time series, that
     * have a timestamp between each of the missing intervals of each measure
     s
     * @param from The start time of range to fetch
     * @param to The end time of range to fetch
     * @param missingIntervalsPerMeasure The sub-ranges missing for each measure
     * @param numberOfGroups The number of groups needed to be fetched for each measure
     */
    AggregatedDataPoints getAggregatedDataPoints(long from, long to, Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
                                                 Map<Integer, Integer> numberOfGroups, QueryMethod queryMethod);

    /**
     * Returns a {@link DataPoints} instance to access the data points in the time series, that
     * have a timestamp greater than or equal to the startTimestamp,
     * and less than or equal to the endTimestamp.
     *
     * @param from The start time of range to fetch
     * @param to The end time of range to fetch
     * @param measures       The measure values to include in every data point
     */
    public DataPoints getDataPoints(long from, long to, List<Integer> measures);

    public DataPoints getDataPoints(long from, long to, Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure);

    /**
     * Returns a {@link DataPoints} instance to access all the data points in the time series.
     *
     * @param measures The measure values to include in every data point
     */
    public DataPoints getAllDataPoints(List<Integer> measures);

}
