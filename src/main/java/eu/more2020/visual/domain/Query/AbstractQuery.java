package eu.more2020.visual.domain.Query;

import eu.more2020.visual.domain.TimeInterval;
import eu.more2020.visual.domain.ViewPort;
import org.apache.hadoop.thirdparty.org.checkerframework.checker.units.qual.K;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;

public abstract class AbstractQuery<T, K> implements TimeInterval {

    final long from;
    final long to;
    final HashMap<Integer, Double[]> filters;
    final ViewPort viewPort;
    K timeColumn;
    List<T> measures;

    public AbstractQuery(long from, long to, List<T> measures, HashMap<Integer, Double[]> filters, ViewPort viewPort) {
        this.from = from;
        this.to = to;
        this.filters = filters;
        this.measures = measures;
        this.viewPort = viewPort;
    }

    public AbstractQuery(long from, long to, List<T> measures, K timeColumn, HashMap<Integer, Double[]> filters, ViewPort viewPort) {
        this.from = from;
        this.to = to;
        this.filters = filters;
        this.timeColumn = timeColumn;
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

    public String getFromDate() {
        return Instant.ofEpochMilli(from).atZone(ZoneId.of("Europe/Athens")).format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss"));
    }

    public String getToDate() {
        return Instant.ofEpochMilli(to).atZone(ZoneId.of("Europe/Athens")).format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss"));
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

    public List<T> getMeasures() {
        return measures;
    }

    public K getTimeColumn() {return timeColumn;}

}
