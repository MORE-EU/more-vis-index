package eu.more2020.visual.domain;

import eu.more2020.visual.util.DateTimeUtil;

import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class SQLQuery {

    Long from;
    long to;
    String timeColumn;
    List<Integer> measures;
    HashMap<Integer, Double[]> filters;
    ViewPort viewPort;

    public SQLQuery(long from, long to, List<Integer> measures, String timeColumn,
                    HashMap<Integer, Double[]> filters, ViewPort viewPort) {
        this.from = from;
        this.to = to;
        this.measures = measures;
        this.timeColumn = timeColumn;
        this.filters = filters;
        this.viewPort = viewPort;
    }

//    public String m4QuerySkeleton() {
//        return
//                "SELECT :timeCol , " +
//                        ":valueCol " + String.join(", ", this.measures) + "\n" +
//                        "FROM :tableName JOIN\n" +
//                        "(SELECT round(:width * (date_part('epoch', :timeCol ) - :from )/(:to - :from )) as k, \n" +
//                        measures.stream().map(m -> "min(" + m + ") as " + m + "_min" ).collect(Collectors.joining(",")) + "\n," +
//                        measures.stream().map(m -> "max(" + m + ") as " + m + "_max" ).collect(Collectors.joining(",")) + "\n," +
//                        "min(date_part('epoch', :timeCol )) as t_min, max(date_part('epoch', :timeCol )) as t_max \n" +
//                        "FROM :tableName GROUP BY k) as QA \n" +
//                        "ON k = round(:width * (date_part('epoch', :timeCol ) - :from )/(:to - :from  )) \n" +
//                        "AND (\n" +
//                        measures.stream().map(m ->  m + "=" + m + "_min OR " + m + "=" + m + "_max" ).collect(Collectors.joining(" AND ")) + "\n" +
//                        "OR (date_part('epoch', :timeCol ) = t_min) OR (date_part('epoch', :timeCol ) = t_max)) ";
//
//    }

    public String m4QuerySkeleton() {
        return
                "WITH\n" +
                        "Q AS (SELECT id, :timeCol , value FROM :tableName \n" +
                        "WHERE id <= 100 AND date_part('epoch', :timeCol ) * 1000 >= :from AND date_part('epoch', :timeCol ) * 1000 <= :to ),\n" +
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

    public Long getFrom() {
        return from;
    }

    public long getTo() {
        return to;
    }

    public String getTimeColumn() {
        return timeColumn;
    }

    public List<Integer> getMeasures() {
        return measures;
    }

    public HashMap<Integer, Double[]> getFilters() {
        return filters;
    }

    public ViewPort getViewPort() {
        return viewPort;
    }

}
