package eu.more2020.visual.middleware.datasource;

import eu.more2020.visual.middleware.domain.MultivariateTimeInterval;
import eu.more2020.visual.middleware.domain.TimeInterval;
import eu.more2020.visual.middleware.domain.TimeRange;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ModelarDBQuery extends DataSourceQuery {
    private final List<List<String>> measureNames;

    public ModelarDBQuery(long from, long to, List<TimeInterval> ranges, List<List<Integer>> measures, List<List<String>> measureNames, int numberOfGroups) {
        super(from, to, ranges, measures, numberOfGroups);
        this.measureNames = measureNames;
    }

    public ModelarDBQuery(long from, long to, List<List<Integer>> measures, List<List<String>> measureNames, int numberOfGroups) {
        super(from, to, new ArrayList<>(List.of(new TimeRange(from, to))), measures, numberOfGroups);
        this.measureNames = measureNames;
    }

    public ModelarDBQuery(long from, long to, List<TimeInterval> ranges, List<List<Integer>> measures, List<List<String>> measureNames){
        super(from, to, ranges, measures,  null);
        this.measureNames = measureNames;
    }

    public ModelarDBQuery(long from, long to, List<List<Integer>> measures, List<List<String>> measureNames){
        super(from, to, new ArrayList<>(List.of(new TimeRange(from, to))), measures,  null);
        this.measureNames = measureNames;
    }


    private String qM4Skeleton(){
        return "WITH Q_M AS (SELECT :idCol, :timeCol, :valueCol, k FROM :tableName as Q \n" +
                "JOIN " +
                "(SELECT :idCol , floor( \n" +
                "(:timeCol - :from ) / ((:to - :from ) / :width )) as k, \n" +
                "min(:valueCol ) as v_min, max(valueCol ) as v_max, \n"  +
                "min(:timeCol ) as t_min, max(:timeCol ) as t_max \n"  +
                "FROM :tableName \n" +
                "WHERE :timeCol >= :from AND :timeCol <= :to \n" +
                "AND :idCol IN (" + this.measureNames.stream().map(Object::toString).collect(Collectors.joining(",")) + ") \n" +
                "GROUP BY :idCol, k ) as QA \n"+
                "ON k = floor((:timeCol - :from ) / ((:to - :from ) / :width )) \n" +
                "AND QA.id = Q.id \n" +
                "AND (:valueCol = v_min OR :valueCol = v_max OR \n" +
                "epoch = t_min OR epoch = t_max) \n" +
                "WHERE :timeCol  >= :from AND :timeCol <= :to \n" +
                "AND Q.id IN (" + this.measureNames.stream().map(Object::toString).collect(Collectors.joining(",")) + ")) \n";
    }

    private String qMultiM4Skeleton(){
        return "WITH Q_M AS (SELECT :idCol, :timeCol, :valueCol, k FROM :tableName as Q \n" +
                "JOIN " +
                "(SELECT :idCol , floor( \n" +
                "(:timeCol - :from ) / ((:to - :from ) / :width )) as k, \n" +
                "min(:valueCol ) as v_min, max(valueCol ) as v_max, \n"  +
                "min(:timeCol ) as t_min, max(:timeCol ) as t_max \n"  +
                "FROM :tableName \n" +
                "WHERE " +
                this.ranges.stream().map(r -> "epoch >= " + r.getFrom() + " AND epoch < " + r.getTo() + " AND id IN ("
                        + this.measureNames.stream().map(Object::toString).collect(Collectors.joining(",")) + ") ").collect(Collectors.joining(" OR ")) +
                "GROUP BY :idCol, k ) as QA \n"+
                "ON k = floor((:timeCol - :from ) / ((:to - :from ) / :width )) \n" +
                "AND QA.id = Q.id \n" +
                "AND (:valueCol = v_min OR :valueCol = v_max OR \n" +
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
    public String minMaxQuerySkeleton() {
        return "SELECT :idCol , floor( \n" +
                "(extract(epoch from :timeCol ) * 1000 - :from ) / ((:to - :from ) / :width )) as k, \n" +
                "min(:valueCol ) as v_min, max(:valueCol ) as v_max \n"  +
                "FROM :tableName \n" +
                "WHERE " +
                this.ranges.stream().map(r -> "extract(epoch from :timeCol ) * 1000 >= " + r.getFrom() + " AND extract(epoch from :timeCol ) * 1000 < " + r.getTo() + " AND :idCol IN ("
                        + this.measureNames.stream().map(o -> "'" + o + "'").collect(Collectors.joining(",")) + ")").collect(Collectors.joining(" OR ")) + " " +
                "GROUP BY :idCol , k " +
                "ORDER BY k, :idCol";
    }

    @Override
    public String rawQuerySkeleton() {
        return "SELECT :idCol , :timeCol , :valueCol FROM :tableName \n" +
                "WHERE :timeCol  >= :from AND :timeCol <= :to \n" +
                "AND :idCol IN (" + this.measureNames.stream().map(o -> "'" + o + "'").collect(Collectors.joining(",")) + ") \n" +
                "ORDER BY :timeCol, :idCol";
    }

    @Override
    public String toString() {
        return "ModelarDBQuery{" +
                "from=" + from +
                ", to=" + to +
                ", ranges=" + ranges +
                ", measures=" + measures +
                ", numberOfGroups=" + numberOfGroups +
                ", measureNames=" + measureNames +
                '}';
    }
}
