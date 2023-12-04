package eu.more2020.visual.middleware.domain.PostgreSQL;

import eu.more2020.visual.middleware.datasource.QueryExecutor.SQLQueryExecutor;
import eu.more2020.visual.middleware.domain.DatabaseConnection;
import eu.more2020.visual.middleware.domain.Dataset.AbstractDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class JDBCConnection implements DatabaseConnection {
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
    }

    public JDBCConnection(String host, String user, String password){
        this.host = host;
        this.user = user;
        this.password = password;
    }

    @Override
    public void connect() throws URISyntaxException, SQLException{
        connection = null;
        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager
                    .getConnection(host, user, password);
            LOG.info("Initialized JDBC connection {}", host);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getClass().getName()+": "+e.getMessage());
        }
    }

    private SQLQueryExecutor createQueryExecutor(AbstractDataset dataset) {
         return new SQLQueryExecutor(connection, dataset);
    }


    private SQLQueryExecutor createQueryExecutor() {
        return new SQLQueryExecutor(connection);
    }

    @Override
    public SQLQueryExecutor getQueryExecutor() {
        return this.createQueryExecutor();
    }

    @Override
    public SQLQueryExecutor getQueryExecutor(AbstractDataset dataset) {
        return this.createQueryExecutor(dataset);
    }

    @Override
    public void closeConnection() throws SQLException{
        try {
            connection.close();
        } catch (Exception e) {
            LOG.error(e.getClass().getName()+": "+e.getMessage());
            throw e;
        }
    }

}
