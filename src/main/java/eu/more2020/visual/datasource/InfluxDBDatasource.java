package eu.more2020.visual.datasource;

import com.google.common.collect.Iterators;
import com.influxdb.query.FluxTable;
import eu.more2020.visual.domain.*;
import eu.more2020.visual.domain.Dataset.InfluxDBDataset;
import eu.more2020.visual.domain.InfluxDB.InfluxDBConnection;
import eu.more2020.visual.datasource.QueryExecutor.InfluxDBQueryExecutor;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
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
        return new InfluxDBDatasource.InfluxDBDatapoints(dataset.getTimeRange().getFrom(),
                dataset.getTimeRange().getTo(), measures);
    }

    @Override
    public AggregatedDataPoints getAggregatedDataPoints(long from, long to, List<TimeInterval> ranges,
                                                        List<Integer> measures, int numberOfGroups) {
        return new InfluxDBDatasource.InfluxDBAggregatedDatapoints(from, to, ranges, measures, numberOfGroups);
    }

    final class InfluxDBDatapoints implements DataPoints {

        private final InfluxDBQuery influxDBQuery;

        public InfluxDBDatapoints(long from, long to, List<Integer> measures) {
            List<String> measureNames = measures.stream().map(m -> dataset.getHeader()[m]).collect(Collectors.toList());
            this.influxDBQuery = new InfluxDBQuery(from, to, measures, measureNames);
        }

        @NotNull
        @Override
        public Iterator<DataPoint> iterator() {
            try {
                InfluxDBQueryExecutor influxDBQueryExecutor = influxDBConnection.getSqlQueryExecutor(dataset.getSchema(),
                        dataset.getName(), dataset.getHeader());
                List<FluxTable> fluxTables = influxDBQueryExecutor.executeRawInfluxQuery(influxDBQuery);
                return new InfluxDBDataPointsIterator(influxDBQuery.getMeasureNames(), fluxTables.get(0));
            } catch (Exception e){
                System.out.println("No data in a specified query");
            }
            return Iterators.concat(new Iterator[0]);
        }

        @Override
        public List<Integer> getMeasures() {
            return influxDBQuery.getMeasures();
        }

        @Override
        public long getFrom() {
            return influxDBQuery.getFrom();
        }

        @Override
        public long getTo() {
            return influxDBQuery.getTo();
        }

        @Override
        public String getFromDate() {
            return influxDBQuery.getFromDate();
        }

        @Override
        public String getToDate() {
            return influxDBQuery.getToDate();
        }

        @Override
        public String getFromDate(String format) {
            return Instant.ofEpochMilli(influxDBQuery.getFrom()).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));
        }

        @Override
        public String getToDate(String format) {
            return Instant.ofEpochMilli(influxDBQuery.getTo()).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));
        }

    }

    final class InfluxDBAggregatedDatapoints implements AggregatedDataPoints {

        private final InfluxDBQuery influxDBQuery;

        public InfluxDBAggregatedDatapoints(long from, long to, List<Integer> measures, int numberOfGroups) {
            List<String> measureNames = measures.stream().map(m -> dataset.getHeader()[m]).collect(Collectors.toList());
            this.influxDBQuery = new InfluxDBQuery(from, to, measures, measureNames, numberOfGroups);
        }

        public InfluxDBAggregatedDatapoints(long from, long to, List<TimeInterval> ranges,
                                            List<Integer> measures, int numberOfGroups) {
            List<String> measureNames = measures.stream().map(m -> dataset.getHeader()[m]).collect(Collectors.toList());
            this.influxDBQuery = new InfluxDBQuery(from, to, ranges, measures, measureNames, numberOfGroups);
        }

        @NotNull
        @Override
        public Iterator<AggregatedDataPoint> iterator() {
            InfluxDBQueryExecutor influxDBQueryExecutor = influxDBConnection.getSqlQueryExecutor(dataset.getSchema(), dataset.getName(), dataset.getHeader());
            List<FluxTable> fluxTables = influxDBQueryExecutor.executeM4MultiInfluxQuery(influxDBQuery);
            if(fluxTables.size() == 0) return Collections.emptyIterator();
            return new InfluxDBAggregateDataPointsIterator(influxDBQuery.getMeasureNames(), influxDBQuery.getMeasures(), fluxTables.get(0), influxDBQuery.getNumberOfGroups());

        }

        @Override
        public List<Integer> getMeasures() {
            return influxDBQuery.getMeasures();
        }

        @Override
        public long getFrom() {
            return influxDBQuery.getFrom();
        }

        @Override
        public long getTo() {
            return influxDBQuery.getTo();
        }

        @Override
        public String getFromDate() {
            return influxDBQuery.getFromDate();
        }

        @Override
        public String getToDate() {
            return influxDBQuery.getToDate();
        }

        @Override
        public String getFromDate(String format) {
            return Instant.ofEpochMilli(influxDBQuery.getFrom()).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));
        }

        @Override
        public String getToDate(String format) {
            return Instant.ofEpochMilli(influxDBQuery.getTo()).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));

        }
    }
}