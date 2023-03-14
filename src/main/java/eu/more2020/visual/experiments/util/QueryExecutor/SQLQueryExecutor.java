package eu.more2020.visual.experiments.util.QueryExecutor;

import eu.more2020.visual.domain.Query.AbstractQuery;
import eu.more2020.visual.domain.Query.QueryMethod;
import eu.more2020.visual.domain.Query.SQLQuery;
import eu.more2020.visual.domain.QueryResults;
import eu.more2020.visual.domain.UnivariateDataPoint;
import eu.more2020.visual.experiments.util.NamedPreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

public class SQLQueryExecutor implements  QueryExecutor{

    private static final Logger LOG = LoggerFactory.getLogger(SQLQueryExecutor.class);


    Connection connection;
    String table;
    String schema;
    String path;
    private final String dropFolder = "postgres-drop-queries";
    private final String initFolder = "postgres-init-queries";

    public SQLQueryExecutor(Connection connection, String path, String table, String schema) {
        this.connection = connection;
        this.table = table;
        this.path = path;
        this.schema = schema;
    }

    @Override
    public void execute(AbstractQuery q, QueryMethod method) throws SQLException {
        switch (method){
            case M4:
                executeM4Query(q);
        }
    }

    @Override
    public QueryResults executeM4Query(AbstractQuery q) throws SQLException {
        return executeM4SqlQuery((SQLQuery) q);
    }

    @Override
    public void initialize() throws SQLException {
        InputStream inputStream
                = getClass().getClassLoader().getResourceAsStream(initFolder + "/" + table + ".sql");
        String[] statements = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8))
                .lines()
                .collect(Collectors.joining("\n")).split(";");
        for (String statement : statements){
            LOG.info("Executing: " + statement);
            connection.prepareStatement(statement.replace("%path", path)).executeUpdate();
        }
    }

    @Override
    public void drop() throws SQLException {
        InputStream inputStream
                = getClass().getClassLoader().getResourceAsStream(dropFolder + "/" + table + ".sql");
        String[] statements = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8))
                .lines()
                .collect(Collectors.joining("\n")).split(";");
        for (String statement : statements){
            LOG.info("Executing: " + statement);
            connection.prepareStatement(statement.replace("'%path'", path)).executeUpdate();
        }
    }

    Comparator<UnivariateDataPoint> compareLists = new Comparator<UnivariateDataPoint>() {
        @Override
        public int compare(UnivariateDataPoint s1, UnivariateDataPoint s2) {
            if (s1==null && s2==null) return 0;//swapping has no point here
            if (s1==null) return  1;
            if (s2==null) return -1;
            return (int) (s1.getTimestamp() - s2.getTimestamp());
        }
    };

    private QueryResults executeM4SqlQuery(SQLQuery q) throws SQLException {
        QueryResults queryResults = new QueryResults();
        HashMap<Integer, List<UnivariateDataPoint>> data = new HashMap<>();
        String sql = q.m4QuerySkeleton();
        NamedPreparedStatement preparedStatement = new NamedPreparedStatement(connection, sql);
        preparedStatement.setString("timeCol", q.getTimeColName());
        preparedStatement.setLong("from", q.getFrom());
        preparedStatement.setLong("to", q.getTo());
        preparedStatement.setInt("width", 800);
        preparedStatement.setString("tableName", schema + "." + table);
        String query = preparedStatement.getPreparedStatement().toString()
                .replace("'", "")
                .replace("epoch", "'epoch'");
        LOG.info("Executing Query: \n" + query);
        ResultSet resultSet = connection.prepareStatement(query).executeQuery();
        while(resultSet.next()){
            Integer measure = resultSet.getInt(1);
            Timestamp timestamp = resultSet.getTimestamp(2);
            Double val = resultSet.getDouble(3);
            data.computeIfAbsent(measure, k -> new ArrayList<>()).add(
                    new UnivariateDataPoint(timestamp.toLocalDateTime().atZone(ZoneId.of("UTC")).toInstant().toEpochMilli(), val));
        }
        data.forEach((k, v) -> v.sort(compareLists));
        queryResults.setData(data);
        return queryResults;
    }
}

