package eu.more2020.visual.experiments.util.PostgreSQL;

import eu.more2020.visual.experiments.util.QueryExecutor.SQLQueryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class PostgreSQL {
    private static final Logger LOG = LoggerFactory.getLogger(PostgreSQL.class);

    String config;
    Connection connection;
    private final Properties properties = new Properties();

    public PostgreSQL(String config) {
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
            System.exit(0);
        }
    }
    public SQLQueryExecutor createQueryExecutor(String path, String table) {
         return new SQLQueryExecutor(connection, path, table, properties.getProperty("schema"));
    }

}
