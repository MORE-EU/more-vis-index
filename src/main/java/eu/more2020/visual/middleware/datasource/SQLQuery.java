package eu.more2020.visual.middleware.datasource;

import eu.more2020.visual.middleware.domain.TimeInterval;
import org.apache.arrow.flatbuf.Int;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class SQLQuery extends DataSourceQuery {

    private final Map<Integer, Long> aggregateIntervals;

    public SQLQuery(long from, long to, Map<Integer,List<TimeInterval>> missingTimeIntervalsPerMeasure, Map<Integer, Integer> numberOfGroups) {
        super(from, to, missingTimeIntervalsPerMeasure, numberOfGroups);
        this.aggregateIntervals = new HashMap<>(numberOfGroups.size());
        for(Integer measure : numberOfGroups.keySet()){
            this.aggregateIntervals.put(measure, (to - from) / numberOfGroups.get(measure));
        }
    }


    public SQLQuery(long from, long to, Map<Integer,List<TimeInterval>> missingTimeIntervalsPerMeasure){
        super(from, to, missingTimeIntervalsPerMeasure,  null);
        this.aggregateIntervals = new HashMap<>(numberOfGroups.size());
        for(Integer measure : numberOfGroups.keySet()){
            this.aggregateIntervals.put(measure, (to - from) / numberOfGroups.get(measure));
        }
    }


    private String calculateFilter(TimeInterval range, int measure) {
        return  " (:timeCol >= " + range.getFrom() + " AND :timeCol < " + range.getTo() + " AND :idCol = " + measure + ") \n" ;
    }

    private String rawSkeleton(TimeInterval range, int measure, int i){
        return "SELECT :idCol , :timeCol , :valueCol ," + i + " as u_id FROM :tableName \n" +
                "WHERE " +
                calculateFilter(range, measure);
    }

    private String rawQuerySkeletonCreator() {
        return null;
//        return IntStream.range(0, ranges.size()).mapToObj(idx -> {
////            TimeInterval range = ranges.get(idx);
////            List<Integer> measureOfRange = measures.get(idx);
//            return measureOfRange.stream().map(m -> rawSkeleton(range, m, idx)).collect(Collectors.joining(" UNION ALL "));
//        }).collect(Collectors.joining(" UNION ALL "));
    }

    private String m4Skeleton(TimeInterval range, int measure, int width, int i ){
       return "SELECT Q.:idCol , :timeCol , :valueCol , k, " + i + " as u_id \n" +
                "FROM :tableName as Q \n" +
                "JOIN " +
                "(SELECT :idCol , floor( \n" +
                "(epoch - :from ) / ((:to - :from ) / " + width + " )) as k, \n" +
                "min(:valueCol ) as v_min, max(:valueCol ) as v_max, \n"  +
                "min(:timeCol ) as t_min, max(:timeCol ) as t_max \n"  +
                "FROM :tableName \n" +
                "WHERE \n" +
                calculateFilter(range, measure) +
                "GROUP BY :idCol , k ) as QA \n"+
                "ON k = floor((:timeCol - :from ) / ((:to - :from ) / " + width + " )) \n" +
                "AND QA.id = Q.id \n" +
                "AND (:valueCol = v_min OR :valueCol = v_max OR \n" +
                ":timeCol = t_min OR :timeCol = t_max) \n" +
                "WHERE \n"  +
                "(:timeCol >= " + range.getFrom() + " AND :timeCol < " + range.getTo() + " AND QA." + ":idCol = " + measure + ") \n" ;
    }

    private String m4QuerySkeletonCreator() {
        AtomicInteger idx = new AtomicInteger();
        return missingIntervalsPerMeasure.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream()
                        .map(range -> m4Skeleton(range, entry.getKey(), numberOfGroups.get(entry.getKey()), idx.getAndIncrement()))
                )
                .collect(Collectors.joining(" UNION ALL "));

    }

    private String minMaxSkeleton(TimeInterval range, int measure, int i ) {
        return "SELECT :idCol , floor( \n" +
                "(epoch - " + range.getFrom() + " ) / " + aggregateIntervals.get(measure) + ") as k, \n" +
                "min(:valueCol ) as v_min, max(:valueCol ) as v_max, "  + i + " as u_id \n" +
                "FROM :tableName \n" +
                "WHERE " +
                calculateFilter(range, measure) +
                "GROUP BY :idCol , k \n";
    }

    private String minMaxQuerySkeletonCreator() {
        AtomicInteger idx = new AtomicInteger();
        return missingIntervalsPerMeasure.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream()
                        .map(range -> minMaxSkeleton(range, entry.getKey(), idx.getAndIncrement()))
                )
                .collect(Collectors.joining(" UNION ALL "));

    }

    @Override
    public String rawQuerySkeleton() {
        return rawQuerySkeletonCreator() +
                "ORDER BY u_id, :timeCol , :idCol \n";
    }

    @Override
    public String m4QuerySkeleton() {
        return "WITH Q_M AS (" + m4QuerySkeletonCreator() + ") \n" +
                "SELECT :idCol , MIN(epoch) AS min_time , MAX(epoch) AS max_time, :valueCol , k, u_id FROM Q_M \n" +
                "GROUP BY :idCol , k , :valueCol , u_id \n" +
                "ORDER BY u_id, k, :idCol";
    }

    @Override
    public String minMaxQuerySkeleton() {
        return minMaxQuerySkeletonCreator() +
                " ORDER BY u_id, k, :idCol ";
    }

    public Map<Integer, Long> getAggregateIntervals() {
        return aggregateIntervals;
    }
}
