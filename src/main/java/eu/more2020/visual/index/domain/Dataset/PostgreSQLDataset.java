package eu.more2020.visual.index.domain.Dataset;

import eu.more2020.visual.index.domain.PostgreSQL.JDBCConnection;
import eu.more2020.visual.index.datasource.QueryExecutor.SQLQueryExecutor;
import eu.more2020.visual.index.domain.TimeRange;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class PostgreSQLDataset extends AbstractDataset {

    private final String config;
    private final String schema;
    private final String table;
    private final String timeFormat;

    public PostgreSQLDataset(String config, String schema, String table,
                             String timeFormat, String timeCol) throws SQLException {
        super();
        this.config = config;
        this.table = table;
        this.schema = schema;
        this.timeFormat = timeFormat;
        setTimeCol(timeCol);
        this.fillPostgreSQLDatasetInfo();
    }

    private void fillPostgreSQLDatasetInfo() throws SQLException {
        ResultSet resultSet;
        JDBCConnection postgreSQLConnection =
                new JDBCConnection(config);
        SQLQueryExecutor sqlQueryExecutor = postgreSQLConnection.getSqlQueryExecutor(schema, table);
        // Header query
        String headerQuery = "SELECT * " +
                "FROM information_schema.columns WHERE table_schema = '" + schema  + "' AND table_name = '" + table + "' ORDER BY ordinal_position\n";
        resultSet = sqlQueryExecutor.execute(headerQuery);
        List<String> header = new ArrayList<>();
        while(resultSet.next())
            header.add(resultSet.getString(1));
        setHeader(header.toArray(new String[0]));

        // First date and sampling frequency query
        String firstQuery = "SELECT epoch\n" +
                "FROM " + schema  + "." + table + " \n" +
                "WHERE id = " + getMeasures().get(0) + " \n" +
                "ORDER BY epoch ASC\n" +
                "LIMIT 2;";
        resultSet = sqlQueryExecutor.execute(firstQuery);
        resultSet.next();
        long from = resultSet.getLong(1);
        resultSet.next();
        long second = resultSet.getLong(1);

        setSamplingInterval(Duration.of(second - from, ChronoUnit.MILLIS));
        // Last date query
        String lastQuery = "SELECT epoch\n" +
                "FROM " + schema  + "." + table + "\n" +
                "ORDER BY epoch DESC\n" +
                "LIMIT 1;";
        resultSet = sqlQueryExecutor.execute(lastQuery);
        resultSet.next();
        long to = resultSet.getLong(1);
        setTimeRange(new TimeRange(from, to));
    }

    @Override
    public String getTimeFormat() {
        return timeFormat;
    }

    @Override
    public List<Integer> getMeasures() {
        int[] measures = new int[getHeader().length];
        for(int i = 0; i < measures.length; i++)
            measures[i] = i;
        return Arrays.stream(measures)
                .boxed()
                .collect(Collectors.toList());
    }
    public String getConfig() {
        return config;
    }

}