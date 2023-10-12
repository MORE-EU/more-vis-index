package eu.more2020.visual.index.datasource;
import com.google.common.collect.Iterators;
import eu.more2020.visual.index.domain.*;
import eu.more2020.visual.index.domain.Dataset.PostgreSQLDataset;
import eu.more2020.visual.index.domain.PostgreSQL.JDBCConnection;
import eu.more2020.visual.index.datasource.QueryExecutor.SQLQueryExecutor;
import eu.more2020.visual.index.domain.Query.QueryMethod;
import org.jetbrains.annotations.NotNull;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PostgreSQLDatasource implements DataSource{

    JDBCConnection postgreSQLConnection;
    PostgreSQLDataset dataset;

    public PostgreSQLDatasource(PostgreSQLDataset dataset) {
        this.dataset = dataset;
        this.postgreSQLConnection =
                new JDBCConnection(dataset.getConfig());
    }

    @Override
    public AggregatedDataPoints getAggregatedDataPoints(long from, long to, List<TimeInterval> ranges, QueryMethod queryMethod,
                                                        List<Integer> measures, int numberOfGroups) {
        return new SQLAggregatedDataPoints(from, to, ranges, queryMethod, measures, numberOfGroups);
    }

    @Override
    public DataPoints getDataPoints(long from, long to, List<Integer> measures) {
        List<TimeInterval> timeIntervals = new ArrayList<>();
        timeIntervals.add(new TimeRange(from, to));
        return new PostgreSQLDatasource.SQLDataPoints(from, to, timeIntervals, measures);
    }

    @Override
    public DataPoints getDataPoints(long from, long to, List<TimeInterval> timeIntervals, List<Integer> measures) {
        return new PostgreSQLDatasource.SQLDataPoints(from, to, timeIntervals, measures);
    }

    @Override
    public DataPoints getAllDataPoints(List<Integer> measures) {
        List<TimeInterval> timeIntervals = new ArrayList<>();
        timeIntervals.add(new TimeRange(dataset.getTimeRange().getFrom(), dataset.getTimeRange().getTo()));
        return new PostgreSQLDatasource.SQLDataPoints(dataset.getTimeRange().getFrom(),
                dataset.getTimeRange().getTo(), timeIntervals,  measures);
    }

    public JDBCConnection getConnection() {
        return postgreSQLConnection;
    }

    /**
     * Represents a series of {@link SQLDataPoints} instances.
     * The iterator returned from this class accesses the SQL database to request the data points.
     */
    final class SQLDataPoints implements DataPoints {

        private final SQLQuery sqlQuery;

        public SQLDataPoints(long from, long to, List<TimeInterval> timeIntervals, List<Integer> measures) {
            this.sqlQuery = new SQLQuery(from, to, timeIntervals, measures);
        }

        @NotNull
        public Iterator<DataPoint> iterator() {
            try {
                SQLQueryExecutor sqlQueryExecutor = postgreSQLConnection.getSqlQueryExecutor(dataset.getSchema(), dataset.getTable());
                ResultSet resultSet = sqlQueryExecutor.executeRawMultiSqlQuery(sqlQuery);
                return new PostgreSQLDataPointsIterator(sqlQuery.getMeasures(), resultSet);
            }
            catch(SQLException e) {
                e.printStackTrace();
            }
            return Iterators.concat(new Iterator[0]);
        }

        @Override
        public List<Integer> getMeasures() {
            return sqlQuery.getMeasures();
        }

        @Override
        public String toString() {
            return "PostgreSQLDataPoints{" +
                    "measures=" + sqlQuery.getMeasures() +
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

    final class SQLAggregatedDataPoints implements AggregatedDataPoints {

        private final SQLQuery sqlQuery;

        private final QueryMethod queryMethod;


        public SQLAggregatedDataPoints(long from, long to, List<TimeInterval> ranges, QueryMethod queryMethod,
                                       List<Integer> measures, int numberOfGroups) {
            this.sqlQuery = new SQLQuery(from, to, ranges, measures, numberOfGroups);
            this.queryMethod = queryMethod;
        }

        @NotNull
        public Iterator<AggregatedDataPoint> iterator() {
            try {
                SQLQueryExecutor sqlQueryExecutor = postgreSQLConnection.getSqlQueryExecutor(dataset.getSchema(), dataset.getTable());
                if(queryMethod == QueryMethod.M4_MULTI){
                    ResultSet resultSet = sqlQueryExecutor.executeM4MultiSqlQuery(sqlQuery);
                    return new PostgreSQLAggregateDataPointsIteratorM4(sqlQuery.getFrom(), sqlQuery.getTo(),
                        sqlQuery.getMeasures(), resultSet, sqlQuery.getNumberOfGroups());
                }
                else {
                    ResultSet resultSet = sqlQueryExecutor.executeMinMaxSqlQuery(sqlQuery);
                    return new PostgreSQLAggregateDataPointsIterator(sqlQuery.getFrom(), sqlQuery.getTo(),
                            sqlQuery.getMeasures(), resultSet, sqlQuery.getNumberOfGroups());
                }
            }
            catch(SQLException e) {
                e.printStackTrace();
            }
            return Iterators.concat(new Iterator[0]);
        }

        @Override
        public List<Integer> getMeasures() {
            return sqlQuery.getMeasures();
        }

        @Override
        public String toString() {
            return "PostgreSQLDataPoints{" +
                    "measures=" + sqlQuery.getMeasures() +
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
