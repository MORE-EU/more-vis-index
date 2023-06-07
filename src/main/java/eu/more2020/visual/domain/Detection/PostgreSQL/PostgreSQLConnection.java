package eu.more2020.visual.domain.Detection.PostgreSQL;

import eu.more2020.visual.datasource.QueryExecutor.SQLQueryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class PostgreSQLConnection {
    private static final Logger LOG = LoggerFactory.getLogger(PostgreSQLConnection.class);

    String config;
    Connection connection;
    private final Properties properties = new Properties();

    public PostgreSQLConnection(String config) {
        this.config = config;
        this.connect();
    }

    private void connect() {
        connection = null;
        try {
            InputStream inputStream
                    = getClass().getClassLoader().getResourceAsStream(config);
            properties.load(inputStream);
            connection = DriverManager
                    .getConnection(properties.getProperty("host") ,
                            properties.getProperty("user"), properties.getProperty("password"));
            LOG.info("Initialized PostgreSQL connection");
        } catch (Exception e) {
            LOG.error(e.getClass().getName()+": "+e.getMessage());
        }
    }

    private SQLQueryExecutor createQueryExecutor(String schema, String table) {
         return new SQLQueryExecutor(connection, schema, table);
    }

    public SQLQueryExecutor getSqlQueryExecutor(String schema, String table) {
        return this.createQueryExecutor(schema, table);
    }

    public Connection getConnection() {
        return connection;
    }
}
