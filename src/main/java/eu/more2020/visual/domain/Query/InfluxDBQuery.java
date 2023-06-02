package eu.more2020.visual.domain.Query;

import eu.more2020.visual.domain.AggregateInterval;
import eu.more2020.visual.domain.TimeRange;
import eu.more2020.visual.domain.ViewPort;
import eu.more2020.visual.util.DateTimeUtil;

import java.time.temporal.ChronoField;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class InfluxDBQuery extends AbstractQuery {


    private AggregateInterval aggregateInterval;
    private List<String> measureNames;

    public InfluxDBQuery(long from, long to, List<String> measuresNames,
                         HashMap<Integer, Double[]> filters, ViewPort viewPort, ChronoField groupByField) {
        super(from, to, QueryMethod.M4, viewPort, groupByField);
        this.measureNames = measuresNames;
        this.aggregateInterval = DateTimeUtil.aggregateInterval(DateTimeUtil.M4(from, to, viewPort));
    }

    public InfluxDBQuery(long from, long to, List<String> measuresNames, ViewPort viewPort) {
        super(from, to, viewPort);
        this.aggregateInterval = DateTimeUtil.aggregateInterval(DateTimeUtil.M4(from, to, viewPort));
        this.measureNames = measuresNames;
    }

    public InfluxDBQuery(long from, long to, List<Integer> measures, List<String> measureNames) {
        super(from, to);
        this.measures = measures;
        this.measureNames = measureNames;
    }

    public InfluxDBQuery(long from, long to, List<Integer> measures, List<String> measureNames, AggregateInterval aggregateInterval) {
        super(from, to);
        this.measures = measures;
        this.measureNames = measureNames;
        this.aggregateInterval = aggregateInterval;

    }

    public InfluxDBQuery(long from, long to, List<TimeRange> ranges, List<Integer> measures, List<String> measureNames, AggregateInterval aggregateInterval) {
        super(from, to);
        this.measures = measures;
        this.ranges = ranges;
        this.measureNames = measureNames;
        this.aggregateInterval = aggregateInterval;

    }

    public List<String> getMeasureNames(){
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


    @Override
    public String m4QuerySkeleton() {
        return ("customAggregateWindow = (every, fn, column=\"_value\", timeSrc=\"_time\", timeDst=\"_time\", tables=<-) =>\n" +
                "  tables\n" +
                "    |> window(every:every, offset: %s, createEmpty:true)\n" +
                "    |> fn(column:column)\n" +
                "    |> duplicate(column:timeSrc, as:timeDst)\n" +
                "    |> group()" +
                "\n" +
                "first = from(bucket:\"%s\") \n " +
                "|> range(start:%s, stop:%s) \n" +
                "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") \n" +
                "|> filter(fn: (r) => r[\"_field\"] ==\"" +
                measureNames.stream().map(Object::toString).collect(Collectors.joining("\" or r[\"_field\"] == \"")) +
                "\") \n" +
                "|> customAggregateWindow(every: " + DateTimeUtil.getInfluxDBAggregateWindow(aggregateInterval) + ", fn: first)\n") +
                ("last = from(bucket:\"%s\") " +
                        "|> range(start:%s, stop:%s)\n " +
                        "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\")\n " +
                        "|> filter(fn: (r) => r[\"_field\"] ==\"" +
                        measureNames.stream().map(Object::toString)
                                .collect(Collectors.joining("\" or r[\"_field\"] == \"")) +
                        "\")\n" +
                        " |> customAggregateWindow(every: " + DateTimeUtil.getInfluxDBAggregateWindow(aggregateInterval) + ", fn: last)\n") +
                ("min = from(bucket:\"%s\") " +
                        "|> range(start:%s, stop:%s)\n " +
                        "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") \n" +
                        "|> filter(fn: (r) => r[\"_field\"] ==\"" +
                        measureNames.stream().map(Object::toString)
                                .collect(Collectors.joining("\" or r[\"_field\"] == \"")) +
                        "\")\n" +
                        " |> customAggregateWindow(every: " + DateTimeUtil.getInfluxDBAggregateWindow(aggregateInterval) + ", fn: min)") +
                ("max = from(bucket:\"%s\") " +
                        "|> range(start:%s, stop:%s)\n " +
                        "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") \n" +
                        "|> filter(fn: (r) => r[\"_field\"] ==\"" +
                        measureNames.stream().map(Object::toString)
                                .collect(Collectors.joining("\" or r[\"_field\"] == \"")) +
                        "\")\n" +
                        "|> customAggregateWindow(every: " + DateTimeUtil.getInfluxDBAggregateWindow(aggregateInterval) + ", fn: max)\n") +
                "union(tables: [min, max, last, first]) \n" +
                "|> sort(columns: [\"_time\"], desc: false)\n";
    }

    @Override
    public String m4MultiQuerySkeleton() {
        String s =  "customAggregateWindow = (every, fn, column=\"_value\", timeSrc=\"_time\", timeDst=\"_time\", tables=<-) =>\n" +
                "  tables\n" +
                "    |> window(every:every, offset: %s , createEmpty:true)\n" +
                "    |> fn(column:column)\n" +
                "    |> duplicate(column:timeSrc, as:timeDst)\n" +
                "    |> group()\n";
        String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        int i = 0;
        for (TimeRange r : ranges) {
            s += ("first_" + i + "= from(bucket:\"%s\") \n " +
                    "|> range(start:" + r.getFromDate(format) + ", stop:" + r.getToDate(format) + ") \n" +
                    "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") \n" +
                    "|> filter(fn: (r) => r[\"_field\"] ==\"" +
                    measureNames.stream().map(Object::toString).collect(Collectors.joining("\" or r[\"_field\"] == \"")) +
                    "\") \n" +
                    "|> customAggregateWindow(every: " + DateTimeUtil.getInfluxDBAggregateWindow(aggregateInterval) + ", fn: first)\n") +
                    ("last_" + i + "= from(bucket:\"%s\") " +
                            "|> range(start:" + r.getFromDate(format) + ", stop:" + r.getToDate(format) + ") \n" +
                            "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\")\n " +
                            "|> filter(fn: (r) => r[\"_field\"] ==\"" +
                            measureNames.stream().map(Object::toString)
                                    .collect(Collectors.joining("\" or r[\"_field\"] == \"")) +
                            "\")\n" +
                            " |> customAggregateWindow(every: " + DateTimeUtil.getInfluxDBAggregateWindow(aggregateInterval) + ", fn: last)\n") +
                    ("min_" + i + " = from(bucket:\"%s\") " +
                            "|> range(start:" + r.getFromDate(format) + ", stop:" + r.getToDate(format) + ") \n" +
                            "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") \n" +
                            "|> filter(fn: (r) => r[\"_field\"] ==\"" +
                            measureNames.stream().map(Object::toString)
                                    .collect(Collectors.joining("\" or r[\"_field\"] == \"")) +
                            "\")\n" +
                            " |> customAggregateWindow(every: " + DateTimeUtil.getInfluxDBAggregateWindow(aggregateInterval) + ", fn: min)\n") +
                    ("max_" + i + " = from(bucket:\"%s\") " +
                            "|> range(start:" + r.getFromDate(format) + ", stop:" + r.getToDate(format) + ") \n" +
                            "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") \n" +
                            "|> filter(fn: (r) => r[\"_field\"] ==\"" +
                            measureNames.stream().map(Object::toString)
                                    .collect(Collectors.joining("\" or r[\"_field\"] == \"")) +
                            "\")\n" +
                            "|> customAggregateWindow(every: " + DateTimeUtil.getInfluxDBAggregateWindow(aggregateInterval) + ", fn: max)\n");
            i++;
        }
        s += "union(tables: [";
        for (i = 0; i < ranges.size(); i ++)
            s +=  "min_" + i + ", max_" + i + ", last_" + i + ", first_" + i + ",";
        s = s.substring(0, s.length() - 1);
        s += "])";
        s += "|> sort(columns: [\"_time\"], desc: false)\n";

        return s;
    }

    @Override
    public String m4WithOLAPQuerySkeleton() {
        return  m4QuerySkeleton() +
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

    public AggregateInterval getAggregateInterval() {
        return aggregateInterval;
    }
}
