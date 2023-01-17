package eu.more2020.visual.domain.Query;

import eu.more2020.visual.domain.ViewPort;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;

public class Query extends AbstractQuery<Integer, Integer> {


    public Query(long from, long to, List<Integer> measures, HashMap<Integer, Double[]> filters, ViewPort viewPort) {
       super(from, to, measures, filters, viewPort);
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
