package eu.more2020.visual.domain;


/**
 * Represents a data point that aggregates a series of raw, non-aggregated data points, along with their aggregated measure values
 */
public interface AggregatedDataPoint extends DataPoint {

    /**
     * @return The number of raw data points represented by this aggregated data point.
     */
    public int getCount();

    /**
     * @return An array of {@link DoubleSummaryStatistics} instances,
     * corresponding to the aggregate measure metadata of the raw data points
     * represented by this aggregated data point.
     */
    public Stats getStats();

}
