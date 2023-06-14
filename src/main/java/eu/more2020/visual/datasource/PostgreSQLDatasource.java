package eu.more2020.visual.datasource;
import com.google.common.collect.Iterators;
import eu.more2020.visual.domain.*;
import eu.more2020.visual.domain.Dataset.PostgreSQLDataset;
import eu.more2020.visual.domain.Detection.PostgreSQL.PostgreSQLConnection;
import eu.more2020.visual.datasource.QueryExecutor.SQLQueryExecutor;
import org.jetbrains.annotations.NotNull;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;

public class PostgreSQLDatasource implements DataSource{

    PostgreSQLConnection postgreSQLConnection;
    PostgreSQLDataset dataset;

    public PostgreSQLDatasource(PostgreSQLDataset dataset) {
        this.dataset = dataset;
        this.postgreSQLConnection =
                new PostgreSQLConnection(dataset.getConfig());
    }

    @Override
    public AggregatedDataPoints getAggregatedDataPoints(long from, long to, List<TimeInterval> ranges,
                                                              List<Integer> measures, int numberOfGroups) {
        return new SQLAggregatedDataPoints(from, to, ranges, measures, numberOfGroups);
    }

    @Override
    public DataPoints getDataPoints(long from, long to, List<Integer> measures) {
        return new PostgreSQLDatasource.SQLDataPoints(from, to, measures);
    }

    @Override
    public DataPoints getAllDataPoints(List<Integer> measures) {
        return new PostgreSQLDatasource.SQLDataPoints(dataset.getTimeRange().getFrom(),
                dataset.getTimeRange().getTo(), measures);
    }

    public PostgreSQLConnection getConnection() {
        return postgreSQLConnection;
    }

    /**
     * Represents a series of {@link SQLDataPoints} instances.
     * The iterator returned from this class accesses the SQL database to request the data points.
     */
    final class SQLDataPoints implements DataPoints {

        private final SQLQuery sqlQuery;

        public SQLDataPoints(long from, long to, List<Integer> measures) {
            this.sqlQuery = new SQLQuery(from, to, measures);
        }

        @NotNull
        public Iterator<DataPoint> iterator() {
            try {
                SQLQueryExecutor sqlQueryExecutor = postgreSQLConnection.getSqlQueryExecutor(dataset.getSchema(), dataset.getName());
                ResultSet resultSet = sqlQueryExecutor.executeRawSqlQuery(sqlQuery);
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


        public SQLAggregatedDataPoints(long from, long to, List<Integer> measures, int numberOfGroups) {
            this.sqlQuery = new SQLQuery(from, to, measures, numberOfGroups);
        }

        public SQLAggregatedDataPoints(long from, long to, List<TimeInterval> ranges,
                                       List<Integer> measures, int numberOfGroups) {
            this.sqlQuery = new SQLQuery(from, to, ranges, measures, numberOfGroups);
        }

        @NotNull
        public Iterator<AggregatedDataPoint> iterator() {

            try {
                SQLQueryExecutor sqlQueryExecutor = postgreSQLConnection.getSqlQueryExecutor(dataset.getSchema(), dataset.getName());
                ResultSet resultSet = sqlQueryExecutor.executeMinMaxSqlQuery(sqlQuery);
                return new PostgreSQLAggregateDataPointsIterator(sqlQuery.getFrom(), sqlQuery.getTo(),
                        sqlQuery.getMeasures(), resultSet, sqlQuery.getNumberOfGroups());
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
