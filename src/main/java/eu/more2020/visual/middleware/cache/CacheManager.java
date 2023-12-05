package eu.more2020.visual.middleware.cache;

import eu.more2020.visual.middleware.domain.Query.Query;
import org.h2.api.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CacheManager {

    private final List<Integer> measures;
    private static final Logger LOG = LoggerFactory.getLogger(MinMaxCache.class);

    private final List<IntervalTree<TimeSeriesSpan>> intervalTrees;

    public CacheManager(List<Integer> measures) {
        this.measures = measures;
        this.intervalTrees = new ArrayList<>();
        measures.forEach(m -> intervalTrees.add(new IntervalTree<>()));
    }

    public void addToCache(List<TimeSeriesSpan> timeSeriesSpans) {
        timeSeriesSpans.forEach(timeSeriesSpan -> getIntervalTree(timeSeriesSpan.getMeasure()).insert(timeSeriesSpan));
    }

    public boolean areListsEqual(List<Integer> list1, List<Integer> list2){
        Collections.sort(list1);
        Collections.sort(list2);
        return list1.equals(list2);
    }

    public List<List<TimeSeriesSpan>> getFromCache(Query query, long pixelColumnInterval) {
        // For each query measure, get the corresponding interval tree. From it retrieve the overlapping spans.
        return query.getMeasures().stream().map(m ->  StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(getIntervalTree(m).overlappers(query), 0), false)
                // Keep only spans with an aggregate interval that is half or less than the pixel column interval to ensure at least one fully contained in every pixel column that the span fully overlaps
                // This way, each of the groups of the resulting spans will overlap at most two pixel columns.
                .filter(span -> pixelColumnInterval >= 2 * span.getAggregateInterval())
                .collect(Collectors.toList())).collect(Collectors.toList());
    }

    protected IntervalTree<TimeSeriesSpan> getIntervalTree(int measure) {
        return intervalTrees.get(measures.indexOf(measure));
    }
}
