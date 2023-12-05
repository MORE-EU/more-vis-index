package eu.more2020.visual.middleware.datasource;

import eu.more2020.visual.middleware.domain.TimeInterval;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Represents a time series data source query
 */
public abstract class DataSourceQuery implements TimeInterval {

    final long from;
    final long to;

    final List<List<TimeInterval>> ranges;
    final List<Integer> measures;

    final int[] numberOfGroups;

    /**
     * Creates a new instance of {@link DataSourceQuery}
     *
     * @param from           The start time of the time interval that was requested
     * @param to             The end time of the time interval that was requested
     * @param ranges         The actual sub-ranges that are missing from the cache and need to be fetched for each measure
     * @param measures       The measure values for which to fetch data
     * @param numberOfGroups The number of groups to aggregate the data points into
     */
    public DataSourceQuery(long from, long to, List<List<TimeInterval>> ranges, List<Integer> measures, int[] numberOfGroups) {
        this.from = from;
        this.to = to;
        this.ranges = ranges;
        this.measures = measures;
        this.numberOfGroups = numberOfGroups;
    }


    @Override
    public long getFrom() {
        return from;
    }

    @Override
    public long getTo() {
        return to;
    }

    @Override
    public String getFromDate() {
        return getFromDate("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public String getFromDate(String format) {
        return Instant.ofEpochMilli(from).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));
    }

    @Override
    public String getToDate() {
        return getToDate("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public String getToDate(String format) {
        return Instant.ofEpochMilli(to).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));
    }

    public int[] getNumberOfGroups() {
        return numberOfGroups;
    }

    public List<Integer> getMeasures() {
        return measures;
    }

    public abstract String m4QuerySkeleton();

    public abstract String minMaxQuerySkeleton();

    public abstract String rawQuerySkeleton();

    public List<List<TimeInterval>> getRanges() {
        return ranges;
    }

}


