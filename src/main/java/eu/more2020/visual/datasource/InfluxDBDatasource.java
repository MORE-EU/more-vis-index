package eu.more2020.visual.datasource;

import com.influxdb.query.FluxTable;
import eu.more2020.visual.domain.AggregateInterval;
import eu.more2020.visual.domain.AggregatedDataPoints;
import eu.more2020.visual.domain.DataPoint;
import eu.more2020.visual.domain.DataPoints;
import eu.more2020.visual.domain.Dataset.InfluxDBDataset;
import eu.more2020.visual.domain.InfluxDB.InfluxDBConnection;
import eu.more2020.visual.util.DateTimeUtil;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class InfluxDBDatasource implements DataSource {

    InfluxDBConnection influxDBConnection;
    InfluxDBDataset dataset;

    public InfluxDBDatasource(InfluxDBDataset dataset) {
        this.dataset = dataset;
        this.influxDBConnection = new InfluxDBConnection(dataset.getConfig());
    }

    @Override
    public DataPoints getDataPoints(long from, long to, List<Integer> measures) {
        return new InfluxDBDatasource.InfluxDBDatapoints(from, to, measures);
    }

    @Override
    public DataPoints getAllDataPoints(List<Integer> measures) {
        return new InfluxDBDatasource.InfluxDBDatapoints(dataset.getTimeRange().getFrom(), dataset.getTimeRange().getTo(), measures);
    }

    @Override
    public AggregatedDataPoints getAggregatedDataPoints(long from, long to, List<Integer> measures, AggregateInterval aggregateInterval) {
        return null;
    }

    final class InfluxDBDatapoints implements DataPoints {

        private final List<Integer> measures;

        private final long from;

        private final long to;

        public InfluxDBDatapoints(long from, long to, List<Integer> measures) {
            this.from = from;
            this.to = to;
            this.measures = measures;
        }

        @NotNull
        @Override
        public Iterator<DataPoint> iterator() {
            List<String> measureNames = measures.stream().map(m -> dataset.getHeader()[m]).collect(Collectors.toList());
            String flux = "from(bucket:\"" + dataset.getBucket() + "\")\n" +
                    "  |> range(start: " + DateTimeUtil.formatTimeStamp(dataset.getTimeFormat(), from).replace(" ", "T") + "Z"
                    + " ,stop: " + DateTimeUtil.formatTimeStamp(dataset.getTimeFormat(), to).replace(" ", "T") + "Z"  + ")\n" +
                    "  |> filter(fn: (r) => r[\"_measurement\"] == \"" + dataset.getMeasurement() + "\")\n" +
                    "  |> filter(fn: (r) => r[\"_field\"] ==\"" +
                    measureNames.stream().map(Object::toString).collect(Collectors.joining("\" or r[\"_field\"] == \"")) +
                    "\")\n" +
                    "|> pivot(rowKey:[\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")";
            FluxTable table = influxDBConnection.getSqlQueryExecutor(dataset.getBucket(), dataset.getMeasurement()).execute(flux).get(0);
            return new InfluxDBDataPointsIterator(measureNames, table);
        }

        @Override
        public List<Integer> getMeasures() {
            return measures;
        }

        @Override
        public long getFrom() {
            return from;
        }

        @Override
        public long getTo() {
            return to;
        }
        @Override
        public String getFromDate() {
            return Instant.ofEpochMilli(from).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        }

        @Override
        public String getToDate() {
            return Instant.ofEpochMilli(to).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        }

    }
}