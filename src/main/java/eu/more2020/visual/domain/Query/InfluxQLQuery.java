package eu.more2020.visual.domain.Query;

import eu.more2020.visual.domain.ViewPort;

import java.util.HashMap;
import java.util.List;

public class InfluxQLQuery extends AbstractQuery<Integer, String>{


    public InfluxQLQuery(long from, long to, List<Integer> measures, String timeColumn,
                         HashMap<Integer, Double[]> filters, ViewPort viewPort) {
        super(from, to, measures, timeColumn, filters,viewPort);
    }


    @Override
    public String m4QuerySkeleton() {
        return ("from(bucket:\"%s\") " +
                "|> range(start:2018-02-03T00:18:00Z, stop:2018-09-05T00:18:00Z) " +
                "|> filter(fn: (r) => r[\"_field\"] == \"value\")" +
                "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") " +
                "|> aggregateWindow(every: 6h, fn: mean, createEmpty: false)" +
                "|> yield(name: \"mean\")");
    }


}
