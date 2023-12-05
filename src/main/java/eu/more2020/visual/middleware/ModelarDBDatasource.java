package eu.more2020.visual.middleware;

import com.google.common.collect.Iterators;
import eu.more2020.visual.middleware.datasource.DataSource;
import eu.more2020.visual.middleware.datasource.ModelarDB.ModelarDBAggregateDataPointsIterator;
import eu.more2020.visual.middleware.datasource.ModelarDB.ModelarDBDataPointsIterator;
import eu.more2020.visual.middleware.datasource.ModelarDBQuery;
import eu.more2020.visual.middleware.datasource.QueryExecutor.ModelarDBQueryExecutor;
import eu.more2020.visual.middleware.domain.*;
import eu.more2020.visual.middleware.domain.Dataset.ModelarDBDataset;
import eu.more2020.visual.middleware.domain.Query.QueryMethod;
import cfjd.org.apache.arrow.flight.FlightStream;
import org.jetbrains.annotations.NotNull;

import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class ModelarDBDatasource implements DataSource {

    ModelarDBQueryExecutor queryExecutor;
    ModelarDBDataset dataset;

    public ModelarDBDatasource(ModelarDBQueryExecutor queryExecutor, ModelarDBDataset dataset) {
        this.dataset = dataset;
        this.queryExecutor = queryExecutor;
    }

    @Override
    public AggregatedDataPoints getAggregatedDataPoints(long from, long to, List<TimeInterval> ranges,
                                                        List<List<Integer>> measures, QueryMethod queryMethod, int numberOfGroups) {
        return new ModelarDBAggregatedDataPoints(from, to, ranges, queryMethod, measures, numberOfGroups);
    }

    @Override
    public DataPoints getDataPoints(long from, long to, List<Integer> measures) {
        List<TimeInterval> timeIntervals = new ArrayList<>();
        List<List<Integer>> allMeasures = new ArrayList<>();
        allMeasures.add(measures);
        timeIntervals.add(new TimeRange(from, to));
        return new ModelarDBDatasource.ModelarDBDataPoints(from, to, timeIntervals, allMeasures);
    }

    @Override
    public DataPoints getDataPoints(long from, long to, List<TimeInterval> timeIntervals, List<List<Integer>> measures) {
        return new ModelarDBDatasource.ModelarDBDataPoints(from, to, timeIntervals, measures);
    }

    @Override
    public DataPoints getAllDataPoints(List<Integer> measures) {
        List<TimeInterval> timeIntervals = new ArrayList<>();
        List<List<Integer>> allMeasures = new ArrayList<>();
        allMeasures.add(measures);
        timeIntervals.add(new TimeRange(dataset.getTimeRange().getFrom(), dataset.getTimeRange().getTo()));
        return new ModelarDBDatasource.ModelarDBDataPoints(dataset.getTimeRange().getFrom(),
                dataset.getTimeRange().getTo(), timeIntervals, allMeasures);
    }

    /**
     * Represents a series of {@link ModelarDBDataPoints} instances.
     * The iterator returned from this class accesses the SQL database to request the data points.
     */
    final class ModelarDBDataPoints implements DataPoints {

        private final ModelarDBQuery modelarDBQuery;

        public ModelarDBDataPoints(long from, long to, List<TimeInterval> timeIntervals, List<List<Integer>> measures) {
            List<List<String>> measureNames = measures.stream().map(m -> m.stream().map(mm -> dataset.getHeader()[mm]).collect(Collectors.toList())).collect(Collectors.toList());
            this.modelarDBQuery = new ModelarDBQuery(from, to, timeIntervals, measures, measureNames);
        }

        @NotNull
        public Iterator<DataPoint> iterator() {
            try {
                FlightStream flightStream = queryExecutor.executeRawModelarDBQuery(modelarDBQuery);
                return new ModelarDBDataPointsIterator(modelarDBQuery.getMeasures(),
                        dataset.getTimeCol(), dataset.getValueCol(), dataset.getIdCol(), flightStream);
            }
            catch(SQLException e) {
                e.printStackTrace();
            }
            return Iterators.concat(new Iterator[0]);
        }

        @Override
        public List<Integer> getMeasures() {
            return null;
        }

        @Override
        public String toString() {
            return "ModelarDBDataPoints{" +
                    "measures=" + modelarDBQuery.getMeasures() +
                    ", from=" + modelarDBQuery.getFrom() +
                    ", to=" + modelarDBQuery.getTo() +
                    '}';
        }

        @Override
        public long getFrom() {
            return modelarDBQuery.getFrom();
        }

        @Override
        public long getTo() {
            return modelarDBQuery.getTo();
        }

        @Override
        public String getFromDate() {
            return getFromDate("yyyy-MM-dd HH:mm:ss");
        }

        @Override
        public String getToDate() {
            return getToDate("yyyy-MM-dd HH:mm:ss");
        }

        @Override
        public String getFromDate(String format) {
            return Instant.ofEpochMilli(modelarDBQuery.getFrom()).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));
        }

        @Override
        public String getToDate(String format) {
            return Instant.ofEpochMilli(modelarDBQuery.getFrom()).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));
        }
    }

    final class ModelarDBAggregatedDataPoints implements AggregatedDataPoints {

        private final ModelarDBQuery modelarDBQuery;

        private final QueryMethod queryMethod;


        public ModelarDBAggregatedDataPoints(long from, long to, List<TimeInterval> ranges, QueryMethod queryMethod,
                                       List<List<Integer>> measures, int numberOfGroups) {
            List<List<String>> measureNames = measures.stream().map(m -> m.stream().map(mm -> dataset.getHeader()[mm]).collect(Collectors.toList())).collect(Collectors.toList());
            this.modelarDBQuery = new ModelarDBQuery(from, to, ranges, measures, measureNames, numberOfGroups);
            this.queryMethod = queryMethod;
        }

        @NotNull
        public Iterator<AggregatedDataPoint> iterator() {
            try {
                FlightStream flightStream = queryExecutor.executeMinMaxModelarDBQuery(modelarDBQuery);
                return new ModelarDBAggregateDataPointsIterator(modelarDBQuery.getFrom(), modelarDBQuery.getTo(),
                        modelarDBQuery.getMeasures(),  dataset.getTimeCol(), dataset.getValueCol(), dataset.getIdCol(), flightStream, modelarDBQuery.getNumberOfGroups());
            }
            catch(SQLException e) {
                e.printStackTrace();
            }
            return Iterators.concat(new Iterator[0]);
        }

        @Override
        public List<Integer> getMeasures() {
            return null;
        }

        @Override
        public String toString() {
            return "PostgreSQLDataPoints{" +
                    "measures=" + modelarDBQuery.getMeasures() +
                    ", from=" + modelarDBQuery.getFrom() +
                    ", to=" + modelarDBQuery.getTo() +
                    '}';
        }

        @Override
        public long getFrom() {
            return modelarDBQuery.getFrom();
        }

        @Override
        public long getTo() {
            return modelarDBQuery.getTo();
        }

        @Override
        public String getFromDate() {
            return getFromDate("yyyy-MM-dd HH:mm:ss");
        }

        @Override
        public String getToDate() {
            return getToDate("yyyy-MM-dd HH:mm:ss");
        }

        @Override
        public String getFromDate(String format) {
            return Instant.ofEpochMilli(modelarDBQuery.getFrom()).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));
        }

        @Override
        public String getToDate(String format) {
            return Instant.ofEpochMilli(modelarDBQuery.getFrom()).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));
        }
    }
}