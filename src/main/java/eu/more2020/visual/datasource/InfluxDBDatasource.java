package eu.more2020.visual.datasource;

import com.google.common.collect.Iterators;
import com.influxdb.query.FluxTable;
import eu.more2020.visual.domain.*;
import eu.more2020.visual.domain.Dataset.InfluxDBDataset;
import eu.more2020.visual.domain.InfluxDB.InfluxDBConnection;
import eu.more2020.visual.domain.Query.InfluxDBQuery;
import eu.more2020.visual.domain.QueryExecutor.InfluxDBQueryExecutor;
import eu.more2020.visual.util.DateTimeUtil;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
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
    public AggregatedDataPoints getAggregatedDataPoints(long from, long to, List<TimeRange> ranges,
                                                        List<Integer> measures, AggregateInterval aggregateInterval) {
        return new InfluxDBDatasource.InfluxDBAggregatedDatapoints(from, to, ranges, measures, aggregateInterval);
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
                InfluxDBQueryExecutor influxDBQueryExecutor = influxDBConnection.getSqlQueryExecutor(dataset.getBucket(),
                        dataset.getMeasurement(), dataset.getHeader());
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
        private AggregateInterval aggregateInterval;

        public InfluxDBAggregatedDatapoints(long from, long to, List<Integer> measures, AggregateInterval aggregateInterval) {
            List<String> measureNames = measures.stream().map(m -> dataset.getHeader()[m]).collect(Collectors.toList());
            this.influxDBQuery = new InfluxDBQuery(from, to, measures, measureNames, aggregateInterval);
            this.aggregateInterval = aggregateInterval;
        }

        public InfluxDBAggregatedDatapoints(long from, long to, List<TimeRange> ranges,
                                            List<Integer> measures, AggregateInterval aggregateInterval) {
            List<String> measureNames = measures.stream().map(m -> dataset.getHeader()[m]).collect(Collectors.toList());
            this.influxDBQuery = new InfluxDBQuery(from, to, ranges, measures, measureNames, aggregateInterval);
            this.aggregateInterval = aggregateInterval;
        }

        @NotNull
        @Override
        public Iterator<AggregatedDataPoint> iterator() {
            InfluxDBQueryExecutor influxDBQueryExecutor = influxDBConnection.getSqlQueryExecutor(dataset.getBucket(), dataset.getMeasurement(), dataset.getHeader());
            List<FluxTable> fluxTables = influxDBQueryExecutor.executeM4MultiInfluxQuery(influxDBQuery);
            if(fluxTables.size() == 0) return Collections.emptyIterator();
            return new InfluxDBAggregateDataPointsIterator(influxDBQuery.getMeasureNames(), influxDBQuery.getMeasures(), fluxTables.get(0));

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