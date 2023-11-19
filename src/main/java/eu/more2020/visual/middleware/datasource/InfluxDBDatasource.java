
package eu.more2020.visual.middleware.datasource;

import com.google.common.collect.Iterators;
import com.influxdb.query.FluxTable;
import eu.more2020.visual.middleware.datasource.InfluxDB.InfluxDBAggregateDataPointsIterator;
import eu.more2020.visual.middleware.datasource.InfluxDB.InfluxDBAggregateDataPointsIteratorM4;
import eu.more2020.visual.middleware.datasource.InfluxDB.InfluxDBDataPointsIterator;
import eu.more2020.visual.middleware.domain.*;
import eu.more2020.visual.middleware.domain.Dataset.InfluxDBDataset;
import eu.more2020.visual.middleware.datasource.QueryExecutor.InfluxDBQueryExecutor;
import eu.more2020.visual.middleware.domain.Query.QueryMethod;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class InfluxDBDatasource implements DataSource {

    InfluxDBQueryExecutor influxDBQueryExecutor;
    InfluxDBDataset dataset;
    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBDatasource.class);

    public InfluxDBDatasource(InfluxDBQueryExecutor influxDBQueryExecutor, InfluxDBDataset dataset) {
        this.dataset = dataset;
        this.influxDBQueryExecutor = influxDBQueryExecutor;
    }

    @Override
    public DataPoints getDataPoints(long from, long to, List<Integer> measures) {
        List<TimeInterval> timeIntervals = new ArrayList<>();
        timeIntervals.add(new TimeRange(from, to));
        List<List<Integer>> allMeasures = new ArrayList<>();
        allMeasures.add(measures);
        return new InfluxDBDatasource.InfluxDBDatapoints(from, to, timeIntervals, allMeasures);
    }

    @Override
    public DataPoints getDataPoints(long from, long to, List<TimeInterval> timeIntervals, List<List<Integer>> measures) {
        return new InfluxDBDatasource.InfluxDBDatapoints(from, to, timeIntervals, measures);

    }

    @Override
    public DataPoints getAllDataPoints(List<Integer> measures) {
        List<TimeInterval> timeIntervals = new ArrayList<>();
        List<List<Integer>> allMeasures = new ArrayList<>();
        allMeasures.add(measures);
        timeIntervals.add(new TimeRange(dataset.getTimeRange().getFrom(), dataset.getTimeRange().getTo()));
        return new InfluxDBDatasource.InfluxDBDatapoints(dataset.getTimeRange().getFrom(),
                dataset.getTimeRange().getTo(), timeIntervals, allMeasures);
    }

    @Override
    public AggregatedDataPoints getAggregatedDataPoints(long from, long to, List<TimeInterval> ranges,  List<List<Integer>> measures, QueryMethod queryMethod, int numberOfGroups) {
        return new InfluxDBDatasource.InfluxDBAggregatedDatapoints(from, to, ranges, queryMethod, measures, numberOfGroups);
    }

    final class InfluxDBDatapoints implements DataPoints {

        private final InfluxDBQuery influxDBQuery;

        public InfluxDBDatapoints(long from, long to, List<TimeInterval> ranges, List<List<Integer>> measures) {
            List<List<String>> measureNames = measures.stream().map(m -> m.stream().map(mm -> dataset.getHeader()[mm]).collect(Collectors.toList())).collect(Collectors.toList());
            this.influxDBQuery = new InfluxDBQuery(from, to, ranges, measures, measureNames);
        }

        @Override
        public List<Integer> getMeasures() {
            return null;
        }

        @Override
        public long getFrom() {
            return influxDBQuery.getFrom();
        }

        @Override
        public long getTo() {
            return influxDBQuery.getTo();
        }

        @NotNull
        @Override
        public Iterator<DataPoint> iterator() {
            try {
                List<FluxTable> fluxTables;
                if(influxDBQuery.getRanges().size() == 1){
                    InfluxDBQuery influxDBQueryRaw = new InfluxDBQuery(influxDBQuery.getRanges().get(0).getFrom(),
                            influxDBQuery.getRanges().get(0).getTo(), influxDBQuery.getRanges(), influxDBQuery.getMeasures(),
                            influxDBQuery.getMeasureNames());
                    fluxTables = influxDBQueryExecutor.executeRawInfluxQuery(influxDBQueryRaw);
                }
                else{
                    fluxTables = influxDBQueryExecutor.executeRawInfluxQuery(influxDBQuery);
                }
                return new InfluxDBDataPointsIterator(influxDBQuery.getRanges(), influxDBQuery.getMeasureNames(), fluxTables);
            } catch (Exception e){
                System.out.println("No data in a specified query");
            }
            return Iterators.concat(new Iterator[0]);
        }
    }

    final class InfluxDBAggregatedDatapoints implements AggregatedDataPoints {

        private final InfluxDBQuery influxDBQuery;
        private final QueryMethod queryMethod;

        public InfluxDBAggregatedDatapoints(long from, long to, List<TimeInterval> ranges,
                                            QueryMethod queryMethod, List<List<Integer>> measures, int numberOfGroups) {
            List<List<String>> measureNames = measures.stream().map(m -> m.stream().map(mm -> dataset.getHeader()[mm]).collect(Collectors.toList())).collect(Collectors.toList());
            this.influxDBQuery = new InfluxDBQuery(from, to, ranges, measures, measureNames, numberOfGroups);
            this.queryMethod = queryMethod;
        }

        @NotNull
        @Override
        public Iterator<AggregatedDataPoint> iterator() {
            if(queryMethod == QueryMethod.M4){
                List<FluxTable> fluxTables = influxDBQueryExecutor.executeM4InfluxQuery(influxDBQuery);
                if(fluxTables.size() == 0) return Collections.emptyIterator();
                return new InfluxDBAggregateDataPointsIteratorM4(influxDBQuery.getMeasureNames(), influxDBQuery.getMeasures(), fluxTables, influxDBQuery.getNumberOfGroups());
            }
            else {
                List<FluxTable> fluxTables = influxDBQueryExecutor.executeMinMaxInfluxQuery(influxDBQuery);
                if (fluxTables.size() == 0) return Collections.emptyIterator();
                return new InfluxDBAggregateDataPointsIterator(influxDBQuery.getMeasureNames(), influxDBQuery.getMeasures(), fluxTables, influxDBQuery.getNumberOfGroups());
            }
        }

        @Override
        public List<Integer> getMeasures() {
            return null;
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