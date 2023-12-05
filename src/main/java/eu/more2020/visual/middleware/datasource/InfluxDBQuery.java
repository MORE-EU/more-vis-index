package eu.more2020.visual.middleware.datasource;

import eu.more2020.visual.middleware.domain.TimeInterval;
import eu.more2020.visual.middleware.domain.TimeRange;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class InfluxDBQuery extends DataSourceQuery {
    private final List<String> measureNames;

    private final long[] aggregateIntervals;
    private final long[] offsets;

    public InfluxDBQuery(long from, long to, List<List<TimeInterval>> ranges, List<Integer> measures, List<String> measureNames, int[] numberOfGroups) {
        super(from, to, ranges, measures, numberOfGroups);
        this.measureNames = measureNames;
        this.aggregateIntervals = new long[numberOfGroups.length];
        this.offsets = new long[numberOfGroups.length];
        for(int i = 0; i < aggregateIntervals.length; i ++){
            this.aggregateIntervals[i] = (to - from) / numberOfGroups[i];
            this.offsets[i] = from % aggregateIntervals[i];
        }
    }

    public InfluxDBQuery(long from, long to, List<Integer> measures, List<String> measureNames, int[] numberOfGroups) {
        this(from, to, List.of(List.of(new TimeRange(from, to))), measures, measureNames, numberOfGroups);
    }

    public InfluxDBQuery(long from, long to, List<Integer> measures, List<String> measureNames) {
        this(from, to, List.of(List.of(new TimeRange(from, to))), measures, measureNames, null);
    }

    public InfluxDBQuery(long from, long to, List<List<TimeInterval>> ranges, List<Integer> measures, List<String> measureNames) {
        this(from, to, ranges, measures, measureNames, null);
    }



    public final List<String> getMeasureNames() {
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

    public long[] getAggregateIntervals() {
        return aggregateIntervals;
    }

    @Override
    public String minMaxQuerySkeleton() {
        String format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
        String s =
                "aggregate = (tables=<-, agg, name, aggregateInterval, offset) => tables" +
                        "\n" +
                        "|> aggregateWindow(every: aggInterval, createEmpty:true, offset: offset, fn: agg, timeSrc:\"_start\")" +
                        "\n";

        int i = 0;
        for (String measureName : measureNames) {
            for(TimeInterval range : ranges.get(i)) {
                s += "data_" + i + " = () => from(bucket:\"%s\") \n" +
                        "|> range(start:" + range.getFromDate(format) + ", stop:" + range.getToDate(format) + ")\n" +
                        "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") \n" +
                        "|> filter(fn: (r) => r[\"_field\"] ==\"" + measureName + ")\n";
            }
            i++;
        }
        s += "union(\n" +
                "    tables: [\n";
        i = 0;
        for (String measureName : measureNames) {
            for (TimeInterval range : ranges.get(i)) {
                s += "data_" + i + "() |> aggregate(agg: max, name: \"max\", offset: " + offsets[i] + ",aggregateInterval:" + aggregateIntervals[i] + ") |> group(columns: [\"_stop\"]),\n" +
                      "data_" + i + "() |> aggregate(agg: min, name: \"min\", offset: " + offsets[i] + ",aggregateInterval:" + aggregateIntervals[i] + ")  |> group(columns: [\"_stop\"]),\n";
            }
            s+= "])\n";
        }
        s += "|> sort(columns: [\"_time\"], desc: false)\n" ;
        return s;
    }


    @Override
    public String m4QuerySkeleton() {
        String format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
        String s = "customAggregateWindow = (every, fn, column=\"_value\", timeSrc=\"_time\", timeDst=\"_time\", aggregateInterval, offset, tables=<-) =>\n" +
                "  tables\n" +
                "    |> window(every:every, offset: offset, createEmpty:true)\n" +
                "    |> fn(column:column)\n" +
                "    |> group()" +
                "\n" +
                "aggregate = (tables=<-, agg, name) => tables" +
                "\n" +
                "|> customAggregateWindow(every: aggregateInterval, fn: agg)" +
                "\n";

        int i = 0;
        for (String measureName : measureNames) {
            for (TimeInterval range : ranges.get(i)) {
                s += "data_" + i + " = () => from(bucket:\"%s\") \n" +
                        "|> range(start:" + range.getFromDate(format) + ", stop:" + range.getToDate(format) + ")\n" +
                        "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") \n" +
                        "|> filter(fn: (r) => r[\"_field\"] ==\""  + measureName + ")\n";
            }
            i++;
        }
        s += "union(\n" +
                "    tables: [\n";

        i = 0;
        for (String measureName : measureNames) {
            for (TimeInterval range : ranges.get(i)) {
                s += "data_" + i + "() |> aggregate(agg: first, name: \"first\", offset: " + offsets[i] + ",aggregateInterval:" + aggregateIntervals[i] + "),\n" +
                        "data_" + i + "() |> aggregate(agg: max, name: \"max\"), offset: " + offsets[i] + ",aggregateInterval:" + aggregateIntervals[i] + "),\n" +
                        "data_" + i + "() |> aggregate(agg: min, name: \"min\"), offset: " + offsets[i] + ",aggregateInterval:" + aggregateIntervals[i] + "),\n" +
                        "data_" + i + "() |> aggregate(agg: last, name: \"last\" offset: " + offsets[i] + ",aggregateInterval:" + aggregateIntervals[i] + "),\n";
            }
            s += "])\n";
        }
         s+= "|> sort(columns: [\"_time\"], desc: false)\n";
        return s;
    }


    @Override
    public String rawQuerySkeleton() {
        int i = 0;
        String format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
        String s = "";

//        int i = 0;
//        for (String measureName : measureNames) {
//            for (TimeInterval range : ranges.get(i)) {
//            s += "data_" + i + " = () => from(bucket:\"%s\") \n" +
//                    "|> range(start:" + r.getFromDate(format) + ", stop:" + r.getToDate(format) + ")\n" +
//                    "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") \n" +
//                    "|> filter(fn: (r) => r[\"_field\"] ==\"" +
//                    measureNames.get(i).stream().map(Object::toString).collect(Collectors.joining("\" or r[\"_field\"] == \"")) + "\")" +
//                    " \n";
//                i++;
//        }
//            s += "union(\n" +
//                    "    tables: [\n";
//            for(i = 0; i < ranges.size(); i ++){
//                s +=    "data_" + i + "(),\n";
//            }
//            s+= "])\n " +
//                    "|>pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")";
//        }

        return s;
    }

}
