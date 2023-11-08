package eu.more2020.visual.middleware.cache;

import eu.more2020.visual.middleware.domain.*;
import eu.more2020.visual.middleware.domain.Dataset.AbstractDataset;
import eu.more2020.visual.middleware.domain.Query.Query;
import eu.more2020.visual.middleware.util.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PrefetchManager {

    private final AbstractDataset dataset;
    private final double prefetchingFactor;
    private final DataProcessor dataProcessor;
    private final CacheManager cacheManager;
    private static final Logger LOG = LoggerFactory.getLogger(MinMaxCache.class);

    public PrefetchManager(AbstractDataset dataset, double prefetchingFactor,
                           CacheManager cacheManager, DataProcessor dataProcessor) {
        this.prefetchingFactor = prefetchingFactor;
        this.cacheManager = cacheManager;
        this.dataProcessor = dataProcessor;
        this.dataset = dataset;
    }

    long[] extendInterval(long from, long to, int width, double factor){
        long pixelColumnInterval = (to - from) / width;
        int noOfColumns = (int) ((width * factor) / 2);
        long newFrom = Math.max(dataset.getTimeRange().getFrom(), from - (noOfColumns * pixelColumnInterval));
        long newTo = Math.min(dataset.getTimeRange().getTo(), (noOfColumns * pixelColumnInterval) + to);
        return new long[]{newFrom, newTo};
    }

    public List<TimeInterval> prefetch(long from, long to, List<Integer> measures,
                                        long pixelColumnInterval, Query query, int aggFactor){
        if(prefetchingFactor == 0) return new ArrayList<>();
        List<TimeInterval> prefetchingIntervals = new ArrayList<>();
        ViewPort viewPort = query.getViewPort();
        // For the prefetching we add pixel columns to the left and right depending to the prefetching factor.
        // We create a new viewport based on the new width that results from prefetching. The new viewport has more columns but the same interval.
        long[] prefetchingInterval = extendInterval(from, to, query.getViewPort().getWidth(), prefetchingFactor);
        long prefetchingFrom = prefetchingInterval[0];
        long prefetchingTo = prefetchingInterval[1];
        prefetchingIntervals.add(new TimeRange(prefetchingFrom, prefetchingTo));
        prefetchingIntervals = DateTimeUtil.groupIntervals(pixelColumnInterval, prefetchingIntervals);

        LOG.info("Prefetching: {}", prefetchingIntervals.stream().map(p -> p.getIntervalString()).collect(Collectors.joining(", ")));
        List<TimeSeriesSpan> missingPrefetchingIntervals = dataProcessor.getMissing(from, to, measures, viewPort, prefetchingIntervals, query, aggFactor);
        cacheManager.addToCache(missingPrefetchingIntervals);
        LOG.info("Inserted new time series spans into interval tree");
        return prefetchingIntervals;
    }
}
