package eu.more2020.visual.util;

import eu.more2020.visual.domain.SQLQuery;
import eu.more2020.visual.experiments.util.NamedPreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SQLQueryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(SQLQueryExecutor.class);


    Connection connection;
    String table;
    String schema;

    public SQLQueryExecutor(Connection connection, String table, String schema) {
        this.connection = connection;
        this.table = table;
        this.schema = schema;
    }

    public void executeM4Query(SQLQuery q) throws SQLException {
        String sql = q.m4QuerySkeleton();
        LOG.info("Executing Query: " + sql);
        NamedPreparedStatement preparedStatement = new NamedPreparedStatement(connection, sql);
        preparedStatement.setString("timeCol", q.getTimeColumn());
        preparedStatement.setLong("from", q.getFrom());
        preparedStatement.setLong("to", q.getTo());
        preparedStatement.setInt("width", q.getViewPort().getWidth());
        preparedStatement.setString("tableName", schema + "." + table);
        String query = preparedStatement.getPreparedStatement().toString()
                .replace("'", "")
                .replace("epoch", "'epoch'");
        LOG.info("Executing Query: \n" + query);
        connection.prepareStatement(query).executeQuery();
    }
}

