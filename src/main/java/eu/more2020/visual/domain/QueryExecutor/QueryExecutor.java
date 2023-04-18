package eu.more2020.visual.domain.QueryExecutor;

import eu.more2020.visual.domain.Query.AbstractQuery;
import eu.more2020.visual.domain.Query.QueryMethod;
import eu.more2020.visual.domain.QueryResults;

import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.List;

public interface QueryExecutor {

    QueryResults execute(AbstractQuery q, QueryMethod method) throws SQLException;
    QueryResults executeM4Query(AbstractQuery q) throws SQLException;
    void drop() throws SQLException, FileNotFoundException;

    QueryResults executeM4OLAPQuery(AbstractQuery q);

    QueryResults executeRawQuery(AbstractQuery q) throws SQLException;

    void initialize(String path) throws SQLException, FileNotFoundException;
}
