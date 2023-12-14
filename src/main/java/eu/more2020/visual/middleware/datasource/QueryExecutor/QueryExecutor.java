package eu.more2020.visual.middleware.datasource.QueryExecutor;

import eu.more2020.visual.middleware.datasource.DataSourceQuery;
import eu.more2020.visual.middleware.domain.Query.QueryMethod;
import eu.more2020.visual.middleware.domain.TableInfo;
import eu.more2020.visual.middleware.domain.QueryResults;

import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.List;

public interface QueryExecutor {

    QueryResults execute(DataSourceQuery q, QueryMethod method) throws SQLException;
    QueryResults executeM4Query(DataSourceQuery q) throws SQLException;
    QueryResults executeRawQuery(DataSourceQuery q) throws SQLException;
    QueryResults executeMinMaxQuery(DataSourceQuery q) throws SQLException;

    void initialize(String path) throws SQLException, FileNotFoundException;
    void drop() throws SQLException, FileNotFoundException;

    List<TableInfo> getTableInfo() throws SQLException;
    List<String> getColumns(String tableName) throws SQLException;
    List<Object[]> getSample(String schema, String tableName) throws SQLException;
}
