package eu.more2020.visual.middleware.datasource;

import eu.more2020.visual.middleware.domain.TimeInterval;
import eu.more2020.visual.middleware.domain.TimeRange;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class InfluxDBQuery extends DataSourceQuery {
    private final List<List<String>> measureNames;
//
    public InfluxDBQuery(long from, long to, List<TimeInterval> ranges, List<List<Integer>> measures, List<List<String>> measureNames, Integer numberOfGroups) {
        super(from, to, ranges, measures, numberOfGroups);
        this.measureNames = measureNames;
        if (numberOfGroups == null) {
            this.aggregateInterval = -1;
        } else {
            this.aggregateInterval = (to - from) / numberOfGroups;
        }
    }

    public InfluxDBQuery(long from, long to, List<List<Integer>> measures, List<List<String>> measureNames, Integer numberOfGroups) {
        this(from, to, new ArrayList<>(List.of(new TimeRange(from, to))), measures, measureNames, numberOfGroups);
    }

    public InfluxDBQuery(long from, long to, List<List<Integer>> measures, List<List<String>> measureNames) {
        this(from, to, new ArrayList<>(List.of(new TimeRange(from, to))), measures, measureNames, null);
    }

    public InfluxDBQuery(long from, long to, List<TimeInterval> ranges, List<List<Integer>> measures, List<List<String>> measureNames) {
        this(from, to, ranges, measures, measureNames, null);
    }

    private final long aggregateInterval;


    public final List<List<String>> getMeasureNames() {
        return measureNames;
    }

    @Override
    public String getFromDate() {
        String format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
        return super.getFromDate(format);
    }

    @Override
    public String getToDate() {
        String format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
        return super.getToDate(format);
    }

    public long getAggregateInterval() {
        return aggregateInterval;
    }

    @Override
    public String minMaxQuerySkeleton() {
        String format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
        String s =
                "aggregate = (tables=<-, agg, name) => tables" +
                        "\n" +
                        "|> aggregateWindow(every:" + aggregateInterval + "ms, createEmpty:true, offset: %s, fn: agg, timeSrc:\"_start\")" +
                        "\n";

        int i = 0;
        for (TimeInterval r : ranges) {
            s += "data_" + i + " = () => from(bucket:\"%s\") \n" +
                    "|> range(start:" + r.getFromDate(format) + ", stop:" + r.getToDate(format) + ")\n" +
                    "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") \n" +
                    "|> filter(fn: (r) => r[\"_field\"] ==\"" +
                    measureNames.get(i).stream().map(Object::toString).collect(Collectors.joining("\" or r[\"_field\"] == \"")) + "\")" +
                    " \n";
            i++;
        }
        s += "union(\n" +
                "    tables: [\n";
        for(i = 0; i < ranges.size(); i ++){
            s +=    "data_" + i + "() |> aggregate(agg: max, name: \"max\") |> group(columns: [\"_stop\"]),\n" +
                    "data_" + i + "() |> aggregate(agg: min, name: \"min\") |> group(columns: [\"_stop\"]),\n";
        }
        s+= "])\n" +
                "|> sort(columns: [\"_time\"], desc: false)\n" ;
        return s;
    }


    @Override
    public String m4QuerySkeleton() {
        String format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
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
                    measureNames.get(i).stream().map(Object::toString).collect(Collectors.joining("\" or r[\"_field\"] == \"")) + "\")" +
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
    public String rawQuerySkeleton() {
        int i = 0;
        String format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
        String s = "";
        if(ranges.size() == 1){
                s = "from(bucket:\"%s\") \n" +
                        "|> range(start:" + ranges.get(0).getFromDate(format) + ", stop:" + ranges.get(0).getToDate(format) + ")\n" +
                        "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") \n" +
                        "|> filter(fn: (r) => r[\"_field\"] ==\"" +
                        measureNames.get(i).stream().map(Object::toString).collect(Collectors.joining("\" or r[\"_field\"] == \"")) + "\")" +
                        " \n" +
                    "|>pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")";
        }
        else {
            for (TimeInterval r : ranges) {
                s += "data_" + i + " = () => from(bucket:\"%s\") \n" +
                        "|> range(start:" + r.getFromDate(format) + ", stop:" + r.getToDate(format) + ")\n" +
                        "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") \n" +
                        "|> filter(fn: (r) => r[\"_field\"] ==\"" +
                        measureNames.get(i).stream().map(Object::toString).collect(Collectors.joining("\" or r[\"_field\"] == \"")) + "\")" +
                        " \n";
                i++;
            }
            s += "union(\n" +
                    "    tables: [\n";
            for(i = 0; i < ranges.size(); i ++){
                s +=    "data_" + i + "(),\n";
            }
            s+= "])\n " +
                    "|>pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")";
        }

        return s;
    }

}
