package eu.more2020.visual.domain;

import java.sql.Date;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;

public class Query implements TimeInterval {

    private final long from;

    private final long to;

    private final HashMap<Integer, Double[]> filters;

    private final List<Integer> measures;

    private final ViewPort viewPort;

    public Query(long from, long to, List<Integer> measures, HashMap<Integer, Double[]> filters, ViewPort viewPort) {
        this.from = from;
        this.to = to;
        this.measures = measures;
        this.filters = filters;
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

    public HashMap<Integer, Double[]> getFilters() {
        return this.filters;
    }


    public List<Integer> getMeasures() {
        return measures;
    }


    public ViewPort getViewPort() {
        return viewPort;
    }

    @Override
    public String toString() {
        return
                "Query{" +
                "from=" + from +
                ", to=" + to +
                ", fromDate=" + Instant.ofEpochMilli(from).atZone(ZoneId.of("Europe/Athens")).format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")) +
                ", toDate=" + Instant.ofEpochMilli(to).atZone(ZoneId.of("Europe/Athens")).format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")) +
                ", filters=" + filters +
                ", measures=" + measures +
                ", viewPort=" + viewPort +
                '}';
    }
}
