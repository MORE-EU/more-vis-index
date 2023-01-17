package eu.more2020.visual.domain.Query;

import eu.more2020.visual.domain.ViewPort;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class SQLQuery extends AbstractQuery<Integer, String>{


    public SQLQuery(long from, long to, List<Integer> measures, String timeColumn,
                    HashMap<Integer, Double[]> filters, ViewPort viewPort) {
        super(from, to, measures, timeColumn, filters, viewPort);
    }


    @Override
    public String m4QuerySkeleton() {
        return
                "WITH\n" +
                        "Q AS (SELECT id, :timeCol , value FROM :tableName \n" +
                        "WHERE id IN (" + this.measures.stream().map(Object::toString).collect(Collectors.joining(",")) + ")" +
                        "AND date_part('epoch', :timeCol ) * 1000 >= :from AND date_part('epoch', :timeCol ) * 1000 <= :to ),\n" +
                        "Q_c AS (SELECT count(*)/count(distinct id) c \n" +
                        "FROM Q),\n" +
                        "Q_r AS (SELECT id,min(:timeCol ),avg(value) FROM Q \n" +
                        "GROUP BY id, floor( \n" +
                        ":width * (date_part('epoch', :timeCol ) * 1000  - (SELECT min(date_part('epoch', :timeCol )* 1000 ) FROM Q)) \n" +
                        "/ (SELECT max(date_part('epoch', :timeCol )) * 1000  - min(date_part('epoch', :timeCol )) * 1000  FROM Q)))\n" +
                        "SELECT * FROM Q\n" +
                        "WHERE (SELECT c FROM Q_c) <= 10000 \n" +
                        "UNION \n" +
                        "SELECT * FROM Q_r \n" +
                        "WHERE (SELECT c FROM Q_c) > 10000";
    }

    @Override
    public String getTimeColumn() {
        return timeColumn;
    }

    @Override
    public List<Integer> getMeasures() {
        return measures;
    }

}
