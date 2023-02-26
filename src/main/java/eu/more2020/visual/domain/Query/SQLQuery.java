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
        return "SELECT Q.id, :timeCol , value FROM :tableName Q \n" +
                "JOIN" +
                "(SELECT id, abs(round( \n" +
                ":width * ((date_part('epoch', :timeCol ) * 1000)  - :from ) / (:to - :from ))) as k, \n" +
                "min(value) as v_min, max(value) as v_max, \n"  +
                "min((date_part('epoch', :timeCol ) * 1000)) as t_min, max((date_part('epoch', :timeCol ) * 1000)) as t_max \n"  +
                "FROM :tableName \n" +
                "WHERE date_part('epoch', :timeCol ) * 1000 >= :from AND date_part('epoch', :timeCol ) * 1000 <= :to \n" +
                "AND id IN (" + this.measures.stream().map(Object::toString).collect(Collectors.joining(",")) + ") \n" +
                "GROUP BY k, id ) as QA \n"+
                "ON k =  abs(round(:width * ((date_part('epoch', :timeCol ) * 1000)  - :from ) / (:to - :from ))) \n" +
                "AND QA.id = Q.id \n" +
                "AND (value = v_min OR value = v_max OR \n" +
                "(date_part('epoch', :timeCol ) * 1000) = t_min OR (date_part('epoch', :timeCol ) * 1000) = t_max) \n" +
                "AND date_part('epoch', :timeCol ) * 1000 >= :from AND date_part('epoch', :timeCol ) * 1000 <= :to";
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
