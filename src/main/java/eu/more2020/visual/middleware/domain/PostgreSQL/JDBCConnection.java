package eu.more2020.visual.middleware.domain.PostgreSQL;

import eu.more2020.visual.middleware.datasource.QueryExecutor.SQLQueryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class JDBCConnection {
    private static final Logger LOG = LoggerFactory.getLogger(JDBCConnection.class);

    String config;
    String host;
    String user;
    String password;
    Connection connection;
    private final Properties properties = new Properties();

    public JDBCConnection(String config) {
        this.config = config;
        InputStream inputStream
                = getClass().getClassLoader().getResourceAsStream(config);
        try {
            properties.load(inputStream);
            host = properties.getProperty("host");
            user = properties.getProperty("user");
            password = properties.getProperty("password");
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.connect();
    }

    public JDBCConnection(String host, String user, String password){

    }

    private void connect() {
        connection = null;
        try {
            connection = DriverManager
                    .getConnection(host, user, password);
            LOG.info("Initialized JDBC connection {}", host);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getClass().getName()+": "+e.getMessage());
        }
    }

    private SQLQueryExecutor createQueryExecutor(String schema, String table) {
         return new SQLQueryExecutor(connection, schema, table);
    }

    public SQLQueryExecutor getSqlQueryExecutor(String schema, String table) {
        return this.createQueryExecutor(schema, table);
    }

    public Connection getConnection(){
        return connection;
    }
}