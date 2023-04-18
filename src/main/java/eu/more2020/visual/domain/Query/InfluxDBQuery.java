package eu.more2020.visual.domain.Query;

import eu.more2020.visual.domain.AggregateInterval;
import eu.more2020.visual.domain.ViewPort;
import eu.more2020.visual.util.DateTimeUtil;

import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class InfluxDBQuery extends AbstractQuery {


    private AggregateInterval aggregateInterval;
    private List<String> measures;

    public InfluxDBQuery(long from, long to, List<String> measures,
                         HashMap<Integer, Double[]> filters, ViewPort viewPort, ChronoField groupByField) {
        super(from, to, viewPort, QueryMethod.M4, groupByField);
        this.measures = measures;
        this.aggregateInterval = DateTimeUtil.aggregateCalendarInterval(DateTimeUtil.optimalM4(from, to, viewPort));
    }

    public InfluxDBQuery(long from, long to, List<String> measures, HashMap<Integer, Double[]> filters,
                         ViewPort viewPort, ChronoField groupByField, AggregateInterval aggregateInterval) {
        super(from, to, viewPort, QueryMethod.M4, groupByField);
        this.measures = measures;
        this.aggregateInterval = aggregateInterval;
    }

    public List<String> getMeasureNames(){
        return measures;
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
        return ("customAggregateWindow = (every, fn, column=\"_value\", timeSrc=\"_time\", timeDst=\"_time\", tables=<-) =>\n" +
                "  tables\n" +
                "    |> window(every:every)\n" +
                "    |> fn(column:column)\n" +
                "    |> duplicate(column:timeSrc, as:timeDst)\n" +
                "    |> group()" +
                "\n" +
                "from(bucket:\"%s\") " +
                "|> range(start:%s, stop:%s) " +
                "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") " +
                "|> filter(fn: (r) => r[\"_field\"] ==\"" +
                measures.stream().map(Object::toString).collect(Collectors.joining("\" or r[\"_field\"] == \"")) +
                "\") " +
                "|> customAggregateWindow(every: " + getAggregateWindow() + ", fn: first)" +
                "|> yield(name: \"first\")") +
                ("from(bucket:\"%s\") " +
                        "|> range(start:%s, stop:%s) " +
                        "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") " +
                        "|> filter(fn: (r) => r[\"_field\"] ==\"" +
                        measures.stream().map(Object::toString)
                                .collect(Collectors.joining("\" or r[\"_field\"] == \"")) +
                        "\")" +
                        " |> customAggregateWindow(every: " + getAggregateWindow() + ", fn: last)" +
                        "|> yield(name: \"last\")") +
                ("from(bucket:\"%s\") " +
                        "|> range(start:%s, stop:%s) " +
                        "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") " +
                        "|> filter(fn: (r) => r[\"_field\"] ==\"" +
                        measures.stream().map(Object::toString)
                                .collect(Collectors.joining("\" or r[\"_field\"] == \"")) +
                        "\")" +
                        " |> customAggregateWindow(every: " + getAggregateWindow() + ", fn: min)" +
                        "|> yield(name: \"min\")") +
                ("from(bucket:\"%s\") " +
                        "|> range(start:%s, stop:%s) " +
                        "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") " +
                        "|> filter(fn: (r) => r[\"_field\"] ==\"" +
                        measures.stream().map(Object::toString)
                                .collect(Collectors.joining("\" or r[\"_field\"] == \"")) +
                        "\") " +
                        "|> customAggregateWindow(every: " + getAggregateWindow() + ", fn: max)" +
                        "|> yield(name: \"max\")");
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
                        "|> mean(column: \"_value\") " +
                        "|> yield(name: \"max\")");
    }

    @Override
    public String rawQuerySkeleton() {
        return ("from(bucket:\"%s\") " +
                "|> range(start:%s, stop:%s) " +
                "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") " +
                "|> filter(fn: (r) => r[\"_field\"] ==\"" +
                measures.stream().map(Object::toString).collect(Collectors.joining("\" or r[\"_field\"] == \"")) +
                "\") ");
    }

}
