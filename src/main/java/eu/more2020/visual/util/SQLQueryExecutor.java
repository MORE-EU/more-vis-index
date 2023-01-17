package eu.more2020.visual.util;

import eu.more2020.visual.domain.Query.AbstractQuery;
import eu.more2020.visual.domain.Query.QueryMethod;
import eu.more2020.visual.domain.Query.SQLQuery;
import eu.more2020.visual.experiments.util.NamedPreparedStatement;
import org.apache.commons.math3.analysis.function.Abs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

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

    public void execute(SQLQuery q, QueryMethod method) throws SQLException {
        switch (method){
            case M4:
                executeM4Query(q);
        }
    }

    private void executeM4Query(SQLQuery q) throws SQLException {
        String sql = q.m4QuerySkeleton();
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

