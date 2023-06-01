package eu.more2020.visual.domain.Query;

import eu.more2020.visual.domain.ViewPort;
import eu.more2020.visual.experiments.util.UserOpType;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.List;

public class Query extends AbstractQuery {

    public Query(long from, long to, float accuracy, QueryMethod queryMethod, List<Integer> measures,
                  ViewPort viewPort, UserOpType opType) {
        super(from, to, accuracy, queryMethod, measures, viewPort, opType);
    }

    public Query(long from, long to, float accuracy, QueryMethod queryMethod, List<Integer> measures,
                 ViewPort viewPort) {
        super(from, to, accuracy, queryMethod, measures, viewPort, null, null, null);
    }

    public Query(long from, long to, List<Integer> measures) {
        super(from, to, 0.9F,
                QueryMethod.M4, measures, new ViewPort(800, 300),
                null, null, null);
    }

    public Query(long from, long to, float accuracy, List<Integer> measures, ViewPort viewPort, UserOpType opType) {
        super(from, to, accuracy, QueryMethod.M4, measures, viewPort, null, null, opType);
    }

    public Query(Long from, Long to, List<Integer> measures, ViewPort viewPort, ChronoField groupyByField) {
        super(from, to, measures, viewPort, groupyByField);
    }

    @Override
    public String m4QuerySkeleton() {
        return null;
    }

    @Override
    public String m4MultiQuerySkeleton() {
        return null;
    }

    @Override
    public String m4WithOLAPQuerySkeleton() {
        return null;
    }

    @Override
    public String rawQuerySkeleton() {
        return null;
    }

    @Override
    public String toString() {
        return
                "Query{" +
                "from=" + from +
                ", to=" + to +
                ", fromDate=" + Instant.ofEpochMilli(from).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")) +
                ", toDate=" + Instant.ofEpochMilli(to).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")) +
                ", filters=" + filters +
                ", measures=" + measures +
                ", viewPort=" + viewPort +
                '}';
    }

}
