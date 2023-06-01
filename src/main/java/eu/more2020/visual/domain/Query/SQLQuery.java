package eu.more2020.visual.domain.Query;

import eu.more2020.visual.domain.TimeRange;
import eu.more2020.visual.domain.ViewPort;

import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class SQLQuery extends AbstractQuery{


    public SQLQuery(long from, long to, List<Integer> measures, ViewPort viewPort, ChronoField groupByField) {
        super(from, to, QueryMethod.M4, measures, viewPort, groupByField);
    }

    public SQLQuery(long from, long to, List<Integer> measures) {
        this(from, to, measures, new ViewPort(800, 300));
    }

    public SQLQuery(long from, long to, List<Integer> measures, ViewPort viewPort) {
        super(from, to, QueryMethod.M4, measures, viewPort);
    }

    public SQLQuery(long from, long to, List<TimeRange> ranges,
                    List<Integer> measures, ViewPort viewPort) {
        super(from, to, ranges, QueryMethod.M4, measures, viewPort);
    }


    private String qM4Skeleton(){
       return "WITH Q_M AS (SELECT Q.id, epoch, value, k FROM :tableName as Q \n" +
                "JOIN " +
                "(SELECT id, abs(round( \n" +
                ":width * (epoch - :from ) / (:to - :from ))) as k, \n" +
                "min(value) as v_min, max(value) as v_max, \n"  +
                "min(epoch) as t_min, max(epoch) as t_max \n"  +
                "FROM :tableName \n" +
                "WHERE epoch >= :from AND epoch <= :to \n" +
                "AND id IN (" + this.measures.stream().map(Object::toString).collect(Collectors.joining(",")) + ") \n" +
                "GROUP BY id, k ) as QA \n"+
                "ON k = abs(round(:width * (epoch - :from ) / (:to - :from ))) \n" +
                "AND QA.id = Q.id \n" +
                "AND (value = v_min OR value = v_max OR \n" +
                "epoch = t_min OR epoch = t_max) \n" +
                "WHERE epoch  >= :from AND epoch <= :to \n" +
                "AND Q.id IN (" + this.measures.stream().map(Object::toString).collect(Collectors.joining(",")) + ")) \n";
    }

    private String qMultiM4Skeleton(){
        return "WITH Q_M AS (SELECT Q.id, epoch, value, k FROM :tableName as Q \n" +
                "JOIN " +
                "(SELECT id, abs(round( \n" +
                ":width * (epoch - :from ) / (:to - :from ))) as k, \n" +
                "min(value) as v_min, max(value) as v_max, \n"  +
                "min(epoch) as t_min, max(epoch) as t_max \n"  +
                "FROM :tableName \n" +
                "WHERE " +
                this.ranges.stream().map(r -> "epoch >= " + r.getFrom() + " AND epoch < " + r.getTo() + " AND id IN ("
                        + this.measures.stream().map(Object::toString).collect(Collectors.joining(",")) + ") ").collect(Collectors.joining(" OR ")) +
                "GROUP BY id, k ) as QA \n"+
                "ON k = abs(round(:width * (epoch - :from ) / (:to - :from ))) \n" +
                "AND QA.id = Q.id \n" +
                "AND (value = v_min OR value = v_max OR \n" +
                "epoch = t_min OR epoch = t_max) \n" +
                "WHERE " +
                this.ranges.stream().map(r -> "epoch >= " + r.getFrom() + " AND epoch < " + r.getTo() + " AND Q.id IN ("
                        + this.measures.stream().map(Object::toString).collect(Collectors.joining(",")) + ")").collect(Collectors.joining(" OR ")) +
                ")\n";
    }


    @Override
    public String m4QuerySkeleton() {
        return qM4Skeleton() +
                "SELECT id, epoch, value, k FROM Q_M " +
                "ORDER BY k, epoch";
    }

    @Override
    public String m4MultiQuerySkeleton() {
        return qMultiM4Skeleton() +
                "SELECT id, epoch, value, k FROM Q_M " +
                "ORDER BY k, epoch";
    }

    @Override
    public String m4WithOLAPQuerySkeleton() {
        return qM4Skeleton() +
                "SELECT id, epoch, value, avg  FROM Q_M \n" +
                "JOIN \n" +
                "(SELECT avg(value), extract(dow from ts) as dow FROM Q_M GROUP BY dow) as Q_G\n" +
                "ON extract(dow from Q_M.ts) = Q_G.dow " +
                "ORDER BY id";
    }

    @Override
    public String rawQuerySkeleton() {
        return "SELECT id, epoch, value FROM :tableName \n" +
                "WHERE epoch  >= :from AND epoch <= :to \n" +
                "AND id IN (" + this.measures.stream().map(Object::toString).collect(Collectors.joining(",")) + ") \n" +
                "ORDER BY epoch, id";
    }



}
