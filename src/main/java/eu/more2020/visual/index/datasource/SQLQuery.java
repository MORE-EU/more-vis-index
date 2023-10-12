package eu.more2020.visual.index.datasource;

import eu.more2020.visual.index.domain.TimeInterval;

import java.util.List;
import java.util.stream.Collectors;

public class SQLQuery extends DataSourceQuery {

    public SQLQuery(long from, long to, List<TimeInterval> ranges, List<Integer> measures, int numberOfGroups) {
        super(from, to, ranges, measures, numberOfGroups);
    }

    public SQLQuery(long from, long to, List<Integer> measures, int numberOfGroups) {
        super(from, to, null, measures, numberOfGroups);
    }

    public SQLQuery(long from, long to, List<TimeInterval> ranges, List<Integer> measures){
        super(from, to, ranges, measures,  null);
    }

    public SQLQuery(long from, long to, List<Integer> measures){
        super(from, to, null, measures,  null);
    }


    private String qM4Skeleton(){
        return "WITH Q_M AS (SELECT Q.id, epoch, value, k FROM :tableName as Q \n" +
                "JOIN " +
                "(SELECT id, floor( \n" +
                "(epoch - :from ) / ((:to - :from ) / :width )) as k, \n" +
                "min(value) as v_min, max(value) as v_max, \n"  +
                "min(epoch) as t_min, max(epoch) as t_max \n"  +
                "FROM :tableName \n" +
                "WHERE epoch >= :from AND epoch <= :to \n" +
                "AND id IN (" + this.measures.stream().map(Object::toString).collect(Collectors.joining(",")) + ") \n" +
                "GROUP BY id, k ) as QA \n"+
                "ON k = floor((epoch - :from ) / ((:to - :from ) / :width )) \n" +
                "AND QA.id = Q.id \n" +
                "AND (value = v_min OR value = v_max OR \n" +
                "epoch = t_min OR epoch = t_max) \n" +
                "WHERE epoch  >= :from AND epoch <= :to \n" +
                "AND Q.id IN (" + this.measures.stream().map(Object::toString).collect(Collectors.joining(",")) + ")) \n";
    }

    private String qMultiM4Skeleton(){
        return "WITH Q_M AS (SELECT Q.id, epoch, value, k FROM :tableName as Q \n" +
                "JOIN " +
                "(SELECT id, floor( \n" +
                "(epoch - :from ) / ((:to - :from )/ :width )) as k, \n" +
                "min(value) as v_min, max(value) as v_max, \n"  +
                "min(epoch) as t_min, max(epoch) as t_max \n"  +
                "FROM :tableName \n" +
                "WHERE " +
                this.ranges.stream().map(r -> "epoch >= " + r.getFrom() + " AND epoch < " + r.getTo() + " AND id IN ("
                        + this.measures.stream().map(Object::toString).collect(Collectors.joining(",")) + ") ").collect(Collectors.joining(" OR ")) +
                "GROUP BY id, k ) as QA \n"+
                "ON k = floor((epoch - :from ) / ((:to - :from ) / :width )) \n" +
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
                "SELECT id, min(epoch) as min_epoch, max(epoch) as max_epoch, value, k FROM Q_M " +
                "GROUP BY id, value, k " +
                "ORDER BY k, min_epoch";
    }

    @Override
    public String m4LikeMultiQuerySkeleton() {
        return qMultiM4Skeleton() +
                "SELECT id, MIN(epoch) AS min_epoch, MAX(epoch) AS max_epoch, value, k FROM Q_M " +
                "GROUP BY id, k, value " +
                "ORDER BY k, min_epoch";
    }

    @Override
    public String m4MultiQuerySkeleton() {
        return qMultiM4Skeleton() +
                "SELECT id, MIN(epoch) AS min_epoch, MAX(epoch) AS max_epoch, value, k FROM Q_M " +
                "GROUP BY id, k, value " +
                "ORDER BY k, min_epoch";
    }

    @Override
    public String minMaxQuerySkeleton() {
        return "SELECT id, floor( \n" +
                "(epoch - :from ) / ((:to - :from ) / :width )) as k, \n" +
                "min(value) as v_min, max(value) as v_max \n"  +
                "FROM :tableName \n" +
                "WHERE " +
                this.ranges.stream().map(r -> "epoch >= " + r.getFrom() + " AND epoch < " + r.getTo() + " AND id IN ("
                        + this.measures.stream().map(Object::toString).collect(Collectors.joining(",")) + ")").collect(Collectors.joining(" OR ")) + " " +
                "GROUP BY id, k " +
                "ORDER BY k, id";
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

    @Override
    public String rawMultiQuerySkeleton() {
        return "SELECT id, epoch, value FROM :tableName \n" +
                "WHERE " +
                this.ranges.stream().map(r -> "epoch >= " + r.getFrom() + " AND epoch < " + r.getTo() + " AND id IN ("
                        + this.measures.stream().map(Object::toString).collect(Collectors.joining(",")) + ") ").collect(Collectors.joining(" OR ")) +
                "ORDER BY epoch, id";
    }


}
