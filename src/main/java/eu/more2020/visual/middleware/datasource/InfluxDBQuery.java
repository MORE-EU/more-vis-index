package eu.more2020.visual.middleware.datasource;

import eu.more2020.visual.middleware.domain.TimeInterval;
import eu.more2020.visual.middleware.domain.TimeRange;

import java.util.*;
import java.util.stream.Collectors;

public class InfluxDBQuery extends DataSourceQuery {
    private final Map<String, List<TimeInterval>>  missingIntervalsPerMeasureName;
    private final Map<String, Long> aggregateIntervals;

    public InfluxDBQuery(long from, long to, Map<String, List<TimeInterval>> missingIntervalsPerMeasureName, Map<String, Integer> numberOfGroups) {
        super(from, to, null, null);
        this.missingIntervalsPerMeasureName = missingIntervalsPerMeasureName;
        this.aggregateIntervals = new HashMap<>(numberOfGroups.size());
        for(String measure : numberOfGroups.keySet()){
            this.aggregateIntervals.put(measure, (to - from) / numberOfGroups.get(measure));
        }
    }


    public InfluxDBQuery(long from, long to, Map<String, List<TimeInterval>> missingIntervalsPerMeasureName) {
        this(from, to, missingIntervalsPerMeasureName, null);
    }


    public Map<String, List<TimeInterval>> getMissingIntervalsPerMeasureName() {
        return missingIntervalsPerMeasureName;
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

    @Override
    public String minMaxQuerySkeleton() {
        String format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
        String s =
                "aggregate = (tables=<-, agg, name, aggregateInterval, offset) => tables" +
                        "\n" +
                        "|> aggregateWindow(every: aggregateInterval, createEmpty:true, offset: offset, fn: agg, timeSrc:\"_start\")" +
                        "\n";

        int i = 0;
        for (String measureName : missingIntervalsPerMeasureName.keySet()) {
            for(TimeInterval range : missingIntervalsPerMeasureName.get(measureName)) {
                s += "data_" + i + " = () => from(bucket:\"%s\") \n" +
                        "|> range(start:" + range.getFromDate(format) + ", stop:" + range.getToDate(format) + ")\n" +
                        "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") \n" +
                        "|> filter(fn: (r) => r[\"_field\"] ==\"" + measureName + "\")\n";
                i++;
            }
        }
        s += "union(\n" +
                "    tables: [\n";
        i = 0;
        for (String measureName : missingIntervalsPerMeasureName.keySet()) {
            for(TimeInterval range : missingIntervalsPerMeasureName.get(measureName)) {
                long rangeOffset = range.getFrom() % aggregateIntervals.get(measureName);
                s += "data_" + i + "() |> aggregate(agg: max, name: \"max\", offset: " + rangeOffset + "ms," + "aggregateInterval:" +  aggregateIntervals.get(measureName) + "ms"+ "),\n" +
                      "data_" + i + "() |> aggregate(agg: min, name: \"min\", offset: " + rangeOffset + "ms," + "aggregateInterval:" + aggregateIntervals.get(measureName) + "ms"+ "),\n";
                i++;
            }
        }
        s+= "])\n";
        s +=    "|> group(columns: [\"_start\", \"_field\"])\n" +
                "|> sort(columns: [\"_time\"], desc: false)\n";
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
        for (String measureName : missingIntervalsPerMeasureName.keySet()) {
            for(TimeInterval range : missingIntervalsPerMeasureName.get(measureName)) {
                s += "data_" + i + " = () => from(bucket:\"%s\") \n" +
                        "|> range(start:" + range.getFromDate(format) + ", stop:" + range.getToDate(format) + ")\n" +
                        "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") \n" +
                        "|> filter(fn: (r) => r[\"_field\"] ==\""  + measureName + ")\n";
                i++;
            }
        }
        s += "union(\n" +
                "    tables: [\n";

        i = 0;
        for (String measureName : missingIntervalsPerMeasureName.keySet()) {

            for(TimeInterval range : missingIntervalsPerMeasureName.get(measureName)) {
                long rangeOffset = range.getFrom() % aggregateIntervals.get(measureName);
                s += "data_" + i + "() |> aggregate(agg: first, name: \"first\", offset: " + rangeOffset + "ms," + "aggregateInterval:" + aggregateIntervals.get(measureName) + "ms" + "),\n" +
                        "data_" + i + "() |> aggregate(agg: max, name: \"max\"), offset: " + rangeOffset + "ms," + "aggregateInterval:" + aggregateIntervals.get(measureName) + "ms" + "),\n" +
                        "data_" + i + "() |> aggregate(agg: min, name: \"min\"), offset: " + rangeOffset + "ms," + "aggregateInterval:" + aggregateIntervals.get(measureName) + "ms" + "),\n" +
                        "data_" + i + "() |> aggregate(agg: last, name: \"last\" offset: " + rangeOffset + "ms," + "aggregateInterval:" + aggregateIntervals.get(measureName) + "ms"+ "),\n";
                i++;
            }
        }
        s += "])\n";
        s +=    "|> group(columns: [\"_start\", \"_field\"])\n" +
                "|> sort(columns: [\"_time\"], desc: false)\n";
        return s;
    }


    @Override
    public String rawQuerySkeleton() {
        String format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
        String s = "";
        int i = 0;
        for (String measureName : missingIntervalsPerMeasureName.keySet()) {
            for(TimeInterval range : missingIntervalsPerMeasureName.get(measureName)) {
                s += "data_" + i + " = () => from(bucket:\"%s\") \n" +
                        "|> range(start:" + range.getFromDate(format) + ", stop:" + range.getToDate(format) + ")\n" +
                        "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") \n" +
                        "|> filter(fn: (r) => r[\"_field\"] ==\"" + measureName + "\")" +
                        " \n";
                i++;
            }
        }
        s += "union(\n" +
                "    tables: [\n";
        for (String measureName : missingIntervalsPerMeasureName.keySet()) {
            for(TimeInterval range : missingIntervalsPerMeasureName.get(measureName)) {
                s += "data_" + i + "(),\n";
            }
        }
        s+= "])\n ";
        s+= "|>pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")";

        return s;
    }

    @Override
    public int getNoOfQueries() {
        return this.getMissingIntervalsPerMeasureName().size() * this.getMissingIntervalsPerMeasureName().values().stream().mapToInt(List::size).sum();

    }



}
