package eu.more2020.visual.domain.Query;

import eu.more2020.visual.domain.Aggregator;
import eu.more2020.visual.domain.TimeInterval;
import eu.more2020.visual.domain.TimeRange;
import eu.more2020.visual.domain.ViewPort;
import eu.more2020.visual.experiments.util.UserOpType;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.List;

public abstract class AbstractQuery implements TimeInterval {

    final long from;
    final long to;
    List<TimeInterval> ranges;
    ViewPort viewPort;
    QueryMethod queryMethod;
    Integer timeColumn;
    List<Integer> measures;
    HashMap<Integer, Double[]> filters;
    UserOpType opType;

    //  the OLAP-type group-by resolution as a ChronoUnit (e.g. ChronoUnit.HOURS)
    ChronoField groupByField;

    Aggregator groupByAggregator;

    float accuracy;

    public AbstractQuery(long from, long to, float accuracy, QueryMethod queryMethod, List<Integer> measures,
                         ViewPort viewPort, HashMap<Integer, Double[]> filters, ChronoField groupByField, UserOpType opType) {
        this.from = from;
        this.to = to;
        this.accuracy = accuracy;
        this.queryMethod = queryMethod;
        this.measures = measures;
        this.filters = filters;
        this.viewPort = viewPort;
        this.groupByField = groupByField;
        this.opType = opType;
    }

    public AbstractQuery(long from, long to, float accuracy, QueryMethod queryMethod, List<Integer> measures,
                         ViewPort viewPort, UserOpType opType) {
        this(from, to, accuracy, queryMethod, measures, viewPort, null, null, opType);
    }

    public AbstractQuery(long from, long to, QueryMethod queryMethod, List<Integer> measures,
                         ViewPort viewPort) {
        this(from, to, 0.9F, queryMethod, measures, viewPort,
                null, null, null);
    }

    public AbstractQuery(long from, long to, QueryMethod queryMethod,
                         ViewPort viewPort, ChronoField groupByField) {
        this(from, to, queryMethod, null, viewPort, groupByField);
    }

    public AbstractQuery(long from, long to, QueryMethod queryMethod, List<Integer> measures,
                         ViewPort viewPort, ChronoField groupByField) {
        this(from, to, queryMethod, measures, viewPort);
        this.groupByField = groupByField;
    }

    public AbstractQuery(long from, long to, List<Integer> measures, ViewPort viewPort, ChronoField chronoField) {
        this(from, to, QueryMethod.M4, measures, viewPort, chronoField);
    }

    public AbstractQuery(long from, long to, List<TimeInterval> ranges, QueryMethod queryMethod,
                         List<Integer> measures, ViewPort viewPort) {
        this(from, to, queryMethod, measures, viewPort);
        this.ranges = ranges;
    }

    public AbstractQuery(long from, long to) {
        this.from = from;
        this.to = to;
    }

    public AbstractQuery(long from, long to, ViewPort viewPort) {
        this.from = from;
        this.to = to;
        this.viewPort = viewPort;
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
        return Instant.ofEpochMilli(from).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    @Override
    public String getToDate() {
        return getToDate("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public String getToDate(String format) {
        return Instant.ofEpochMilli(to).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }


    public HashMap<Integer, Double[]> getFilters() {
        return this.filters;
    }

    public ViewPort getViewPort() {
        return viewPort;
    }

    public List<Integer> getMeasures() {
        return measures;
    }

    public Integer getTimeColumn() {
        return timeColumn;
    }

    public ChronoField getGroupByField() {
        return groupByField;
    }

    public UserOpType getOpType() {
        return opType;
    }

    public void setGroupByResolution(ChronoField groupByField) {
        this.groupByField = groupByField;
    }

    public Aggregator getGroupByAggregator() {
        return groupByAggregator;
    }

    public void setGroupByAggregator(Aggregator groupByAggregator) {
        this.groupByAggregator = groupByAggregator;
    }

    public abstract String m4QuerySkeleton();

    public abstract String m4MultiQuerySkeleton();

    public abstract String m4WithOLAPQuerySkeleton();

    public QueryMethod getQueryMethod() {
        return queryMethod;
    }

    public abstract String rawQuerySkeleton();

    public List<TimeInterval> getRanges() {
        return ranges;
    }

    public float getAccuracy() {
        return accuracy;
    }
}
