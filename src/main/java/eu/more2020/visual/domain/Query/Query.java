package eu.more2020.visual.domain.Query;

import eu.more2020.visual.domain.Aggregator;
import eu.more2020.visual.domain.ViewPort;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;

public class Query extends AbstractQuery {

    public Query(long from, long to, List<Integer> measures,
                 HashMap<Integer, Double[]> filters, ViewPort viewPort, ChronoField groupByField) {
        super(from, to, measures, filters, viewPort, groupByField);
    }
    public Query(long from, long to, List<Integer> measures, Integer timeCol,
                 HashMap<Integer, Double[]> filters, ViewPort viewPort, ChronoField groupByField) {
       super(from, to, measures, timeCol, filters, viewPort, groupByField);
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
