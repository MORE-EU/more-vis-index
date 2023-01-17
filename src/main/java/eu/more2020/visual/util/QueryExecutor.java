package eu.more2020.visual.util;

import eu.more2020.visual.domain.Query.AbstractQuery;
import java.sql.SQLException;

public interface QueryExecutor {

    void executeM4Query(AbstractQuery q) throws SQLException;
}
