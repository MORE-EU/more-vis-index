package eu.more2020.visual.datasource;

import eu.more2020.visual.domain.TimeInterval;

import java.util.List;
import java.util.stream.Collectors;

public class InfluxDBQuery extends DataSourceQuery {
    private final List<String> measureNames;

    public InfluxDBQuery(long from, long to, List<TimeInterval> ranges, List<Integer> measures, List<String> measureNames, Integer numberOfGroups) {
        super(from, to, ranges, measures, numberOfGroups);
        this.measureNames = measureNames;
        if (numberOfGroups == null) {
            this.aggregateInterval = -1;
        } else {
            this.aggregateInterval = (to - from) / numberOfGroups;
        }
    }

    public InfluxDBQuery(long from, long to, List<Integer> measures, List<String> measureNames, Integer numberOfGroups) {
        this(from, to, null, measures, measureNames, numberOfGroups);
    }

    public InfluxDBQuery(long from, long to, List<Integer> measures, List<String> measureNames) {
        this(from, to, null, measures, measureNames, null);
    }

    private final long aggregateInterval;


    public final List<String> getMeasureNames() {
        return measureNames;
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

    public long getAggregateInterval() {
        return aggregateInterval;
    }

    @Override
    public String m4QuerySkeleton() {
        return ("customAggregateWindow = (every, fn, column=\"_value\", timeSrc=\"_time\", timeDst=\"_time\", tables=<-) =>\n" +
                "  tables\n" +
                "    |> window(every:every, offset: %s, createEmpty:true)\n" +
                "    |> fn(column:column)\n" +
//                "    |> duplicate(column:timeSrc, as:timeDst)\n" +
                "    |> group()" +
                "\n" +
                "data = () => from(bucket:\"%s\") \n" +
                "|> range(start:%s, stop:%s) \n" +
                "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") \n" +
                "|> filter(fn: (r) => r[\"_field\"] ==\"" +
                measureNames.stream().map(Object::toString).collect(Collectors.joining("\" or r[\"_field\"] == \"")) + "\")" +
                " \n" +
                "aggregate = (tables=<-, agg, name) =>\n" +
                "    tables\n" +
                "        |> customAggregateWindow(every: " + aggregateInterval + "ms, fn: agg)\n" +
                "\n" +
                "union(\n" +
                "    tables: [\n" +
                "        data() |> aggregate(agg: first, name: \"first\"),\n" +
                "        data() |> aggregate(agg: max, name: \"max\"),\n" +
                "        data() |> aggregate(agg: min, name: \"min\"),\n" +
                "        data() |> aggregate(agg: last, name: \"last\"),\n" +
                "    ],\n" +
                ")" +
                "|> sort(columns: [\"_time\"], desc: false)\n");
    }


//    @Override
//    public String m4MultiQuerySkeleton() {
//        String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";
//        String s =
//                "aggregate = (tables=<-, agg, name) => tables" +
//                "\n" +
//                "|> aggregateWindow(every:" + aggregateInterval + "ms, fn: agg, createEmpty: true, timeSrc:\"_start\")" +
//                "\n";
//        int i = 0;
//        for (TimeInterval r : ranges) {
//            s += "data_" + i + " = () => from(bucket:\"%s\") \n" +
//                    "|> range(start:" + r.getFromDate(format) + ", stop:" + r.getToDate(format) + ")\n" +
//                    "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") \n" +
//                    "|> filter(fn: (r) => r[\"_field\"] ==\"" +
//                    measureNames.stream().map(Object::toString).collect(Collectors.joining("\" or r[\"_field\"] == \"")) + "\")" +
//                    " \n";
//        }
//        s += "union(\n" +
//                "    tables: [\n";
//        for(i = 0; i < ranges.size(); i ++){
//            s +=    "data_" + i + "() |> aggregate(agg: first, name: \"first\"),\n" +
//                    "data_" + i + "() |> aggregate(agg: max, name: \"max\"),\n" +
//                    "data_" + i + "() |> aggregate(agg: min, name: \"min\"),\n" +
//                    "data_" + i + "() |> aggregate(agg: last, name: \"last\"),\n";
//        }
//        s+= "])" +
//                "\n" + "|> sort(columns: [\"_time\"], desc: false)\n";
//        return s;
//    }

    @Override
    public String minMaxQuerySkeleton() {
        String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        String s =
                "aggregate = (tables=<-, agg, name) => tables" +
                        "\n" +
                        "|> aggregateWindow(every:" + aggregateInterval + "ms, offset: %s, fn: agg, timeSrc:\"_start\")" +
                        "\n";

        int i = 0;
        for (TimeInterval r : ranges) {
            s += "data_" + i + " = () => from(bucket:\"%s\") \n" +
                    "|> range(start:" + r.getFromDate(format) + ", stop:" + r.getToDate(format) + ")\n" +
                    "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") \n" +
                    "|> filter(fn: (r) => r[\"_field\"] ==\"" +
                    measureNames.stream().map(Object::toString).collect(Collectors.joining("\" or r[\"_field\"] == \"")) + "\")" +
                    " \n";
            i++;
        }
        s += "union(\n" +
                "    tables: [\n";
        for(i = 0; i < ranges.size(); i ++){
            s +=    "data_" + i + "() |> aggregate(agg: max, name: \"max\"),\n" +
                    "data_" + i + "() |> aggregate(agg: min, name: \"min\"),\n";
        }
        s+= "])" +
                "\n" + "|> sort(columns: [\"_time\"], desc: false)\n";
        return s;
    }

    @Override
    public String m4MultiQuerySkeleton() {
        String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        String s = "customAggregateWindow = (every, fn, column=\"_value\", timeSrc=\"_time\", timeDst=\"_time\", tables=<-) =>\n" +
                "  tables\n" +
                "    |> window(every:every, offset: %s, createEmpty:true)\n" +
                "    |> fn(column:column)\n" +
                "    |> group()" +
                "\n" +
                "aggregate = (tables=<-, agg, name) => tables" +
                "\n" +
                "|> customAggregateWindow(every:" + aggregateInterval + "ms, fn: agg)" +
                "\n";

        int i = 0;
        for (TimeInterval r : ranges) {
            s += "data_" + i + " = () => from(bucket:\"%s\") \n" +
                    "|> range(start:" + r.getFromDate(format) + ", stop:" + r.getToDate(format) + ")\n" +
                    "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") \n" +
                    "|> filter(fn: (r) => r[\"_field\"] ==\"" +
                    measureNames.stream().map(Object::toString).collect(Collectors.joining("\" or r[\"_field\"] == \"")) + "\")" +
                    " \n";
            i++;
        }
        s += "union(\n" +
                "    tables: [\n";
        for(i = 0; i < ranges.size(); i ++){
            s +=    "data_" + i + "() |> aggregate(agg: first, name: \"first\"),\n" +
                    "data_" + i + "() |> aggregate(agg: max, name: \"max\"),\n" +
                    "data_" + i + "() |> aggregate(agg: min, name: \"min\"),\n" +
                    "data_" + i + "() |> aggregate(agg: last, name: \"last\"),\n";
        }
        s+= "])" +
                "\n" + "|> sort(columns: [\"_time\"], desc: false)\n";
        return s;
    }


    @Override
    public String m4WithOLAPQuerySkeleton() {
        return m4QuerySkeleton() +
                ("from(bucket:\"%s\") " +
                        "|> range(start:%s, stop:%s) " +
                        "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") " +
                        "|> filter(fn: (r) => r[\"_field\"] ==\"" +
                        "|> map(fn: (r) => ({ r with hour: date.hour(t: r._time) }))  \n" +
                        "|> group(columns: [\"hour\"], mode:\"by\")\n" +
                        "|> mean(column: \"_value\")\n " +
                        "|> yield(name: \"max\")\n" +
                        "|> group()");
    }

    @Override
    public String rawQuerySkeleton() {
        return ("from(bucket:\"%s\") " +
                "|> range(start:%s, stop:%s) " +
                "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") " +
                "|> filter(fn: (r) => r[\"_field\"] ==\"" +
                measureNames.stream().map(Object::toString).collect(Collectors.joining("\" or r[\"_field\"] == \"")) +
                "\")\n" +
                "|> keep(columns: [\"_measurement\", \"_time\", \"_field\", \"_value\"])\n" +
                "|>pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")");
    }

}
