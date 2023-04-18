package eu.more2020.visual.domain.Query;

import eu.more2020.visual.domain.ViewPort;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.List;

public class Query extends AbstractQuery {

    public Query(long from, long to, List<Integer> measures,
                 HashMap<Integer, Double[]> filters, ViewPort viewPort, ChronoField groupByField) {
        super(from, to, QueryMethod.M4, measures, filters, viewPort, groupByField);
    }

    public Query(long from, long to, QueryMethod queryMethod, List<Integer> measures,
                 HashMap<Integer, Double[]> filters, ViewPort viewPort, ChronoField groupByField) {
        super(from, to, queryMethod, measures,  filters, viewPort, groupByField);
    }

    public Query(long from, long to, List<Integer> measures) {
        super(from, to, QueryMethod.M4, measures);
    }

    @Override
    public String m4QuerySkeleton() {
        return null;
    }

    @Override
    public String m4WithOLAPQuerySkeleton() {
        return null;
    }

    @Override
    public String rawQuerySkeleton() {
        return null;
    }

    @Override
    public String toString() {
        return
                "Query{" +
                "from=" + from +
                ", to=" + to +
                ", fromDate=" + Instant.ofEpochMilli(from).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")) +
                ", toDate=" + Instant.ofEpochMilli(to).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")) +
                ", filters=" + filters +
                ", measures=" + measures +
                ", viewPort=" + viewPort +
                '}';
    }

}
