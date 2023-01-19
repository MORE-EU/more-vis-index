package eu.more2020.visual.util;

import eu.more2020.visual.domain.Query.AbstractQuery;
import eu.more2020.visual.domain.Query.QueryMethod;

import java.sql.SQLException;

public interface QueryExecutor {


    void execute(AbstractQuery q, QueryMethod method) throws SQLException;
    void executeM4Query(AbstractQuery q) throws SQLException;
}
