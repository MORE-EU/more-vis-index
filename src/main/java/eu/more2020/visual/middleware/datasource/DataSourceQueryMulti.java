package eu.more2020.visual.middleware.datasource;

import eu.more2020.visual.middleware.domain.TimeInterval;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Represents a time series data source query
 */
public abstract class DataSourceQueryMulti implements TimeInterval {

    final long from;
    final long to;

    final List<TimeInterval> ranges;
    final List<List<Integer>> measures;

    final Integer numberOfGroups;

    /**
     * Creates a new instance of {@link DataSourceQueryMulti}
     *
     * @param from           The start time of the time interval that was requested
     * @param to             The end time of the time interval that was requested
     * @param ranges         The actual sub-ranges that are missing from the cache and need to be fetched
     * @param measures       The measure values to include in every data point
     * @param numberOfGroups The number of groups to aggregate the data points into
     */
    public DataSourceQueryMulti(long from, long to, List<TimeInterval> ranges, List<List<Integer>> measures, Integer numberOfGroups) {
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

    public Integer getNumberOfGroups() {
        return numberOfGroups;
    }

    public List<List<Integer>> getMeasures() {
        return measures;
    }

    public abstract String m4QuerySkeleton();

    public abstract String m4LikeMultiQuerySkeleton();

    public abstract String m4MultiQuerySkeleton();

    public abstract String minMaxQuerySkeleton();

    public abstract String m4WithOLAPQuerySkeleton();

    public abstract String rawQuerySkeleton();

    public List<TimeInterval> getRanges() {
        return ranges;
    }

    public abstract String rawMultiQuerySkeleton();

}


