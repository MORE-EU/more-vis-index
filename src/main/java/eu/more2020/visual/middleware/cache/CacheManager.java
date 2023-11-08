package eu.more2020.visual.middleware.cache;

import eu.more2020.visual.middleware.domain.Query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CacheManager {
    private static final Logger LOG = LoggerFactory.getLogger(MinMaxCache.class);

    private final IntervalTree<TimeSeriesSpan> intervalTree;

    public CacheManager() {
        this.intervalTree = new IntervalTree<>();
    }

    public void addToCache(TimeSeriesSpan span) {
        intervalTree.insert(span);
    }

    public void addToCache(List<TimeSeriesSpan> timeSeriesSpans) {
        if(timeSeriesSpans != null) intervalTree.insertAll(timeSeriesSpans);
    }

    public boolean areListsEqual(List<Integer> list1, List<Integer> list2){
        Collections.sort(list1);
        Collections.sort(list2);
        return list1.equals(list2);
    }

    public List<TimeSeriesSpan> getFromCache(Query query, long pixelColumnInterval) {
        // Cache retrieval logic here
        return StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(intervalTree.overlappers(query), 0), false)
                // Keep only spans with an aggregate interval that is half or less than the pixel column interval to ensure at least one fully contained in every pixel column that the span fully overlaps
                // This way, each of the groups of the resulting spans will overlap at most two pixel columns.
                .filter(span -> pixelColumnInterval >= 2 * span.getAggregateInterval()
                        && areListsEqual(span.getMeasures(), query.getMeasures())
                )
                .collect(Collectors.toList());
    }

}
