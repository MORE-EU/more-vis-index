package eu.more2020.visual.middleware.domain;

import java.sql.SQLException;


import eu.more2020.visual.middleware.datasource.QueryExecutor.QueryExecutor;
import eu.more2020.visual.middleware.domain.Dataset.AbstractDataset;

public interface DatabaseConnection {

    public void connect() throws  SQLException;


    public QueryExecutor getSqlQueryExecutor();

    public QueryExecutor getSqlQueryExecutor(AbstractDataset dataset);

    public void closeConnection() throws SQLException;
}