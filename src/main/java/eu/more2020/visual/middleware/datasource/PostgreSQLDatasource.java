package eu.more2020.visual.middleware.datasource;
import com.google.common.collect.Iterators;
import eu.more2020.visual.middleware.datasource.PostgreSQL.PostgreSQLAggregateDataPointsIterator;
import eu.more2020.visual.middleware.datasource.PostgreSQL.PostgreSQLAggregateDataPointsIteratorM4;
import eu.more2020.visual.middleware.datasource.PostgreSQL.PostgreSQLDataPointsIterator;
import eu.more2020.visual.middleware.domain.*;
import eu.more2020.visual.middleware.domain.Dataset.PostgreSQLDataset;
import eu.more2020.visual.middleware.datasource.QueryExecutor.SQLQueryExecutor;
import eu.more2020.visual.middleware.domain.Query.QueryMethod;
import org.jetbrains.annotations.NotNull;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class PostgreSQLDatasource implements DataSource {

    SQLQueryExecutor sqlQueryExecutor;
    PostgreSQLDataset dataset;

    public PostgreSQLDatasource(SQLQueryExecutor sqlQueryExecutor, PostgreSQLDataset dataset) {
        this.dataset = dataset;
        this.sqlQueryExecutor = sqlQueryExecutor;
    }

    @Override
    public AggregatedDataPoints getAggregatedDataPoints(long from, long to,
                                                        Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure, Map<Integer, Integer> numberOfGroups, QueryMethod queryMethod) {
        return new SQLAggregatedDataPoints(from, to, missingIntervalsPerMeasure, numberOfGroups, queryMethod);
    }

    @Override
    public DataPoints getDataPoints(long from, long to, List<Integer> measures) {
        Map<Integer, List<TimeInterval>> missingTimeIntervalsPerMeasure = new HashMap<>();
        for (Integer measure : measures) {
            List<TimeInterval> timeIntervalsForMeasure = new ArrayList<>();
            timeIntervalsForMeasure.add(new TimeRange(from, to));
            missingTimeIntervalsPerMeasure.put(measure, timeIntervalsForMeasure);
        }
        return new PostgreSQLDatasource.SQLDataPoints(from, to, missingTimeIntervalsPerMeasure);
    }

    @Override
    public DataPoints getDataPoints(long from, long to, Map<Integer, List<TimeInterval>> missingTimeIntervalsPerMeasure) {
        return new PostgreSQLDatasource.SQLDataPoints(from, to, missingTimeIntervalsPerMeasure);
    }

    @Override
    public DataPoints getAllDataPoints(List<Integer> measures) {
        Map<Integer, List<TimeInterval>> missingTimeIntervalsPerMeasure = new HashMap<>(measures.size());
        for (Integer measure : measures) {
            List<TimeInterval> timeIntervalsForMeasure = new ArrayList<>();
            timeIntervalsForMeasure.add(new TimeRange(dataset.getTimeRange().getFrom(), dataset.getTimeRange().getTo()));
            missingTimeIntervalsPerMeasure.put(measure, timeIntervalsForMeasure);
        }
        return new PostgreSQLDatasource.SQLDataPoints(dataset.getTimeRange().getFrom(),
                dataset.getTimeRange().getTo(), missingTimeIntervalsPerMeasure);
    }

    /**
     * Represents a series of {@link SQLDataPoints} instances.
     * The iterator returned from this class accesses the SQL database to request the data points.
     */

    public class SQLDataPoints implements DataPoints {

        private final SQLQuery sqlQuery;

        public SQLDataPoints(long from, long to, Map<Integer, List<TimeInterval>> missingTimeIntervalsPerMeasure) {
            this.sqlQuery = new SQLQuery(from, to, missingTimeIntervalsPerMeasure);
        }

        @NotNull
        public Iterator<DataPoint> iterator() {
            try {
                ResultSet resultSet = sqlQueryExecutor.executeRawSqlQuery(sqlQuery);
                return new PostgreSQLDataPointsIterator(new ArrayList<>(), resultSet);
            }
            catch(SQLException e) {
                e.printStackTrace();
            }
            return Iterators.concat(new Iterator[0]);
        }

        @Override
        public long getFrom() {
            return sqlQuery.getFrom();
        }

        @Override
        public long getTo() {
            return sqlQuery.getFrom();
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
            return Instant.ofEpochMilli(sqlQuery.getFrom()).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));
        }

        @Override
        public String getToDate(String format) {
            return Instant.ofEpochMilli(sqlQuery.getFrom()).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));
        }

    }

    final class SQLAggregatedDataPoints implements AggregatedDataPoints {

        private final SQLQuery sqlQuery;
        private final QueryMethod queryMethod;


        public SQLAggregatedDataPoints(long from, long to, Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure, Map<Integer, Integer> numberOfGroups, QueryMethod queryMethod) {
            this.sqlQuery = new SQLQuery(from, to, missingIntervalsPerMeasure, numberOfGroups);
            this.queryMethod = queryMethod;
        }

        @NotNull
        public Iterator<AggregatedDataPoint> iterator() {
            try {
                if (queryMethod == QueryMethod.M4) {
                    ResultSet resultSet = sqlQueryExecutor.executeM4SqlQuery(sqlQuery);
                    return new PostgreSQLAggregateDataPointsIteratorM4(resultSet, sqlQuery.getMissingIntervalsPerMeasure(), sqlQuery.getAggregateIntervals());
                } else {
                    ResultSet resultSet = sqlQueryExecutor.executeMinMaxSqlQuery(sqlQuery);
                    return new PostgreSQLAggregateDataPointsIterator(resultSet, sqlQuery.getMissingIntervalsPerMeasure(), sqlQuery.getAggregateIntervals());
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return Iterators.concat(new Iterator[0]);
        }

        @Override
        public String toString() {
            return "PostgreSQLDataPoints{" +
                    "measures=" + sqlQuery.getMissingIntervalsPerMeasure().keySet() +
                    ", from=" + sqlQuery.getFrom() +
                    ", to=" + sqlQuery.getTo() +
                    '}';
        }

        @Override
        public long getFrom() {
            return sqlQuery.getFrom();
        }

        @Override
        public long getTo() {
            return sqlQuery.getTo();
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
            return Instant.ofEpochMilli(sqlQuery.getFrom()).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));
        }

        @Override
        public String getToDate(String format) {
            return Instant.ofEpochMilli(sqlQuery.getFrom()).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));
        }
    }


}
