package eu.more2020.visual.domain.Query;

import eu.more2020.visual.domain.Aggregator;
import eu.more2020.visual.domain.TimeInterval;
import eu.more2020.visual.domain.ViewPort;
import org.apache.hadoop.thirdparty.org.checkerframework.checker.units.qual.K;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;

public abstract class AbstractQuery implements TimeInterval {

    final long from;
    final long to;
    final HashMap<Integer, Double[]> filters;
    final ViewPort viewPort;
    Integer timeColumn;
    List<Integer> measures;

    //  the OLAP-type group-by resolution as a ChronoUnit (e.g. ChronoUnit.HOURS)
    ChronoField groupByField;

    Aggregator groupByAggregator;

    public AbstractQuery(long from, long to, List<Integer> measures, HashMap<Integer, Double[]> filters, ViewPort viewPort, ChronoField groupByField) {
        this.from = from;
        this.to = to;
        this.filters = filters;
        this.measures = measures;
        this.viewPort = viewPort;
        this.groupByField = groupByField;
    }

    public AbstractQuery(long from, long to, List<Integer> measures, Integer timeColumn, HashMap<Integer, Double[]> filters, ViewPort viewPort, ChronoField groupByField) {
        this.from = from;
        this.to = to;
        this.filters = filters;
        this.timeColumn = timeColumn;
        this.measures = measures;
        this.viewPort = viewPort;
        this.groupByField = groupByField;
    }

    public AbstractQuery(long from, long to, List<Integer> measures, Integer timeColumn, HashMap<Integer, Double[]> filters, ViewPort viewPort) {
        this.from = from;
        this.to = to;
        this.filters = filters;
        this.measures = measures;
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
        return Instant.ofEpochMilli(from).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    @Override
    public String getToDate() {
        return Instant.ofEpochMilli(to).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    public String querySkeletons(QueryMethod method) {
        switch (method){
            case M4:
                return m4QuerySkeleton();
            default:
                return "";
        }
    }

    public String m4QuerySkeleton() { return ""; }

    public HashMap<Integer, Double[]> getFilters() {
        return this.filters;
    }

    public ViewPort getViewPort() {
        return viewPort;
    }

    public List<Integer> getMeasures() {
        return measures;
    }

    public Integer getTimeColumn() {return timeColumn;}

    public ChronoField getGroupByField() {
        return groupByField;
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
}
