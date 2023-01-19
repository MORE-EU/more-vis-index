package eu.more2020.visual.domain.Query;

import eu.more2020.visual.domain.ViewPort;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class InfluxQLQuery extends AbstractQuery<String, String>{


    public InfluxQLQuery(long from, long to, List<String> measures, String timeColumn,
                         HashMap<Integer, Double[]> filters, ViewPort viewPort) {
        super(from, to, measures, timeColumn, filters,viewPort);
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


    @Override
    public String m4QuerySkeleton() {
        return ("from(bucket:\"%s\") " +
                    "|> range(start:%s, stop:%s) " +
                    "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") " +
                    "|> filter(fn: (r) => r[\"_field\"] ==\"" +
                    measures.stream().map(Object::toString).collect(Collectors.joining("\" or r[\"_field\"] == \"")) +
                    "\") |> aggregateWindow(every: 6h, fn: first, createEmpty: false)" +
                    "|> yield(name: \"first\")") +
                ("from(bucket:\"%s\") " +
                        "|> range(start:%s, stop:%s) " +
                        "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") " +
                        "|> filter(fn: (r) => r[\"_field\"] ==\"" +
                        measures.stream().map(Object::toString).collect(Collectors.joining("\" or r[\"_field\"] == \"")) +
                        "\") |> aggregateWindow(every: 6h, fn: last, createEmpty: false)" +
                        "|> yield(name: \"last\")") +
                ("from(bucket:\"%s\") " +
                        "|> range(start:%s, stop:%s) " +
                        "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") " +
                        "|> filter(fn: (r) => r[\"_field\"] ==\"" +
                        measures.stream().map(Object::toString).collect(Collectors.joining("\" or r[\"_field\"] == \"")) +
                        "\") |> aggregateWindow(every: 6h, fn: min, createEmpty: false)" +
                        "|> yield(name: \"min\")") +
                ("from(bucket:\"%s\") " +
                        "|> range(start:%s, stop:%s) " +
                        "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") " +
                        "|> filter(fn: (r) => r[\"_field\"] ==\"" +
                        measures.stream().map(Object::toString).collect(Collectors.joining("\" or r[\"_field\"] == \"")) +
                        "\") |> aggregateWindow(every: 6h, fn: max, createEmpty: false)" +
                        "|> yield(name: \"max\")") +
                ("from(bucket:\"%s\") " +
                        "|> range(start:%s, stop:%s) " +
                        "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") " +
                        "|> filter(fn: (r) => r[\"_field\"] ==\"" +
                        measures.stream().map(Object::toString).collect(Collectors.joining("\" or r[\"_field\"] == \"")) +
                        "\") |> aggregateWindow(every: 6h, fn: mean, createEmpty: false)" +
                        "|> yield(name: \"mean\")");
    }


}
