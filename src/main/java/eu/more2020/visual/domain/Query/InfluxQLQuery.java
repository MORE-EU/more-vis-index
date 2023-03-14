package eu.more2020.visual.domain.Query;

import eu.more2020.visual.domain.AggregateInterval;
import eu.more2020.visual.domain.ViewPort;
import eu.more2020.visual.util.DateTimeUtil;

import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class InfluxQLQuery extends AbstractQuery {


    private AggregateInterval aggregateInterval;
    private List<String> measures;
    private String timeColumn;

    public InfluxQLQuery(long from, long to, List<String> measures, String timeColumn,
                         HashMap<Integer, Double[]> filters, ViewPort viewPort, ChronoField groupByField) {
        super(from, to, viewPort, groupByField);
        this.measures = measures;
        this.timeColumn = timeColumn;
        this.aggregateInterval = DateTimeUtil.aggregateCalendarInterval(DateTimeUtil.optimalM4(from, to, viewPort));
    }


    @Override
    public String getFromDate() {
        String fromDate = super.getFromDate();
        return fromDate.replace(" ", "T") + "Z";
    }

    @Override
    public String getToDate() {
        String toDate = super.getToDate();
        return toDate.replace(" ", "T") + "Z";
    }

    public String getAggregateWindow() {
        switch (aggregateInterval.getChronoUnit().toString()) {
            case ("Millis"):
                return aggregateInterval.getInterval() + "ms";
            case ("Seconds"):
                return aggregateInterval.getInterval() + "s";
            case ("Minutes"):
                return aggregateInterval.getInterval() + "m";
            case ("Hours"):
                return aggregateInterval.getInterval() + "h";
            default:
                return "inf";
        }
    }

    @Override
    public String m4QuerySkeleton() {
        return ("from(bucket:\"%s\") " +
                "|> range(start:%s, stop:%s) " +
                "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") " +
                "|> filter(fn: (r) => r[\"_field\"] ==\"" +
                measures.stream().map(Object::toString).collect(Collectors.joining("\" or r[\"_field\"] == \"")) +
                "\") |> aggregateWindow(every: " + getAggregateWindow() + ", fn: first, createEmpty: false)" +
                "|> yield(name: \"first\")") +
                ("from(bucket:\"%s\") " +
                        "|> range(start:%s, stop:%s) " +
                        "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") " +
                        "|> filter(fn: (r) => r[\"_field\"] ==\"" +
                        measures.stream().map(Object::toString)
                                .collect(Collectors.joining("\" or r[\"_field\"] == \"")) +
                        "\") |> aggregateWindow(every: " + getAggregateWindow() + ", fn: last, createEmpty: false)" +
                        "|> yield(name: \"last\")") +
                ("from(bucket:\"%s\") " +
                        "|> range(start:%s, stop:%s) " +
                        "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") " +
                        "|> filter(fn: (r) => r[\"_field\"] ==\"" +
                        measures.stream().map(Object::toString)
                                .collect(Collectors.joining("\" or r[\"_field\"] == \"")) +
                        "\") |> aggregateWindow(every: " + getAggregateWindow() + ", fn: min, createEmpty: false)" +
                        "|> yield(name: \"min\")") +
                ("from(bucket:\"%s\") " +
                        "|> range(start:%s, stop:%s) " +
                        "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") " +
                        "|> filter(fn: (r) => r[\"_field\"] ==\"" +
                        measures.stream().map(Object::toString)
                                .collect(Collectors.joining("\" or r[\"_field\"] == \"")) +
                        "\") |> aggregateWindow(every: " + getAggregateWindow() + ", fn: max, createEmpty: false)" +
                        "|> yield(name: \"max\")");
    }


}
