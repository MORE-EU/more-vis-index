package eu.more2020.visual.datasource.QueryExecutor;

import eu.more2020.visual.datasource.DataSourceQuery;
import eu.more2020.visual.domain.Query.QueryMethod;
import eu.more2020.visual.domain.QueryResults;

import java.io.FileNotFoundException;
import java.sql.SQLException;

public interface QueryExecutor {

    QueryResults execute(DataSourceQuery q, QueryMethod method) throws SQLException;
    QueryResults executeM4Query(DataSourceQuery q) throws SQLException;
    void drop() throws SQLException, FileNotFoundException;

    QueryResults executeM4MultiQuery(DataSourceQuery q) throws SQLException;

    QueryResults executeM4OLAPQuery(DataSourceQuery q) throws SQLException;

    QueryResults executeRawQuery(DataSourceQuery q) throws SQLException;

    QueryResults executeMinMaxQuery(DataSourceQuery q) throws SQLException;

    void initialize(String path) throws SQLException, FileNotFoundException;
}
