package eu.more2020.visual.index;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import eu.more2020.visual.datasource.DataSource;
import eu.more2020.visual.datasource.DataSourceFactory;
import eu.more2020.visual.domain.*;
import eu.more2020.visual.domain.Dataset.AbstractDataset;
import eu.more2020.visual.domain.Query.AbstractQuery;
import eu.more2020.visual.experiments.util.GroupByEvaluator;
import eu.more2020.visual.util.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class TTI {

    private static final Logger LOG = LoggerFactory.getLogger(TTI.class);

    private final AbstractDataset dataset;

    private final DataSource dataSource;

    private final float accuracy = 0.9f;
    Comparator<UnivariateDataPoint> compareLists = new Comparator<UnivariateDataPoint>() {
        @Override
        public int compare(UnivariateDataPoint s1, UnivariateDataPoint s2) {
            if (s1 == null && s2 == null) return 0;//swapping has no point here
            if (s1 == null) return 1;
            if (s2 == null) return -1;
            return (int) (s1.getTimestamp() - s2.getTimestamp());
        }
    };
    // The interval tree containing all the time series spans already cached
    private IntervalTree<TimeSeriesSpan> intervalTree;

    /**
     * Creates a new TTI for a multi measure-time series
     *
     * @param dataset
     */
    public TTI(AbstractDataset dataset) {
        this.dataset = dataset;
        this.dataSource = DataSourceFactory.getDataSource(dataset);
        intervalTree = new IntervalTree<>();
    }

    @SuppressWarnings("UnstableApiUsage")
    public QueryResults executeQuery(AbstractQuery query) {
        LOG.info("Executing query: " + query.getFromDate() + " - " + query.getToDate());
        Duration optimalM4Interval = DateTimeUtil.optimalM4(query.getFrom(), query.getTo(), query.getViewPort());
        AggregateInterval optimalM4AggInterval = DateTimeUtil.aggregateCalendarInterval(optimalM4Interval);
        Duration accurateInterval = DateTimeUtil.accurateCalendarInterval(query.getFrom(), query.getTo(), query.getViewPort(), accuracy);
        accurateInterval = accurateInterval.toMillis() < dataset.getSamplingInterval()
                .toMillis() ? dataset.getSamplingInterval() : accurateInterval;

        AggregateInterval accurateAggInterval = DateTimeUtil.aggregateCalendarInterval(accurateInterval);
        List<Integer> measures = query.getMeasures() == null ? dataset.getMeasures() : query.getMeasures();
        QueryResults queryResults = new QueryResults();
        final int[] ioCount = {0};
        RangeSet<Long> rangeSet = TreeRangeSet.create();

        long from = Math.max(dataset.getTimeRange().getFrom(), (query.getFrom() - (long) (query.getTo() - query.getFrom()) / 2));
        long to = Math.min(dataset.getTimeRange().getTo(), (query.getTo() + (long) (query.getTo() - query.getFrom()) / 2));
        final ImmutableRangeSet<Long>[] currentDifference = new ImmutableRangeSet[]{ImmutableRangeSet.of(Range.closed(from, to))};

        // Sort overlapping spans, by their query coverage. Then find which are the ones covering the whole range, and
        // also keep the remaining difference.
        Duration finalAccurateInterval = accurateInterval;
        List<TimeSeriesSpan> overlappingIntervals = StreamSupport.stream(Spliterators.spliteratorUnknownSize(intervalTree.overlappers(query), 0), false)
                .filter(span -> span.getAggregateInterval().toDuration()
                        .compareTo(finalAccurateInterval) <= 0 && (span.overlaps(query)))
                .sorted(Comparator.comparing(span -> span.percentage(query), Comparator.reverseOrder()))
                .filter(span -> {
                    if (currentDifference[0].isEmpty())
                        return false; // If the difference has been covered, don't check.
                    rangeSet.add(Range.closed(span.getFrom(), span.getTo()));
                    ImmutableRangeSet<Long> newDifference = currentDifference[0].difference(rangeSet);
                    if (!currentDifference[0].equals(newDifference)) { // If the current span, added to the difference, keep it.
                        currentDifference[0] = newDifference;
                        return true;
                    }
                    return false;
                })
                .collect(Collectors.toList());

        // Calculate and add missing intervals
        List<TimeRange> ranges = currentDifference[0].asRanges().stream()
                .map(r -> new TimeRange(r.lowerEndpoint(), r.upperEndpoint())).collect(Collectors.toList());
        if(ranges.size() >= 1) {
            AggregatedDataPoints dataPoints =
                    dataSource.getAggregatedDataPoints(from, to, ranges, measures, accurateAggInterval);
            List<TimeSeriesSpan> timeSeriesSpans = TimeSeriesSpanFactory.create(dataPoints, ranges, accurateAggInterval);
            overlappingIntervals.addAll(timeSeriesSpans);
            intervalTree.insertAll(timeSeriesSpans);
        }
        overlappingIntervals.sort((i1, i2) -> (int) (i1.getFrom() - i2.getFrom())); // Sort intervals

        GroupByEvaluator groupByEvaluator = query.getGroupByField() != null
                ? new GroupByEvaluator(measures, query.getGroupByField())
                : null;
        MultiSpanIterator<TimeSeriesSpan> multiSpanIterator = new MultiSpanIterator(overlappingIntervals.iterator(), groupByEvaluator);
        PixelAggregator pixelAggregator = new PixelAggregator(multiSpanIterator, query.getFrom(), query.getTo(), measures, optimalM4AggInterval, query.getViewPort());
        Map<Integer, List<UnivariateDataPoint>> data = measures.stream()
                .collect(Collectors.toMap(Function.identity(), ArrayList::new));

        while (pixelAggregator.hasNext()) {
            PixelAggregatedDataPoint next = pixelAggregator.next();
            PixelStatsAggregator stats = next.getStats();
            if(stats.getCount() != 0) {
                for (int measure : measures) {
                    List<UnivariateDataPoint> measureData = data.get(measure);
                    measureData.add(new UnivariateDataPoint(stats.getFirstTimestamp(measure), stats.getFirstValue(measure)));
                    measureData.add(new UnivariateDataPoint(stats.getLastTimestamp(measure), stats.getLastValue(measure)));
                    measureData.add(new UnivariateDataPoint(stats.getMinTimestamp(measure), stats.getMinValue(measure)));
                    measureData.add(new UnivariateDataPoint(stats.getMaxTimestamp(measure), stats.getMaxValue(measure)));
                }
            }
        }
//        for (Integer measure : measures) {
//            double error = Double.parseDouble(String.format("%.3f", pixelAggregator.getError(measure) * 100));
//            LOG.info("Query Max Error (" + measure  +"): " + error + "%");
//        }
        data.forEach((k, v) -> v.sort(compareLists));
        queryResults.setData(data);
        queryResults.setIoCount(ioCount[0]);
        return queryResults;
    }

    /**
     * Calculates the deep memory size of this instance.
     *
     * @return The deep memory size in bytes.
     */
    public long calculateDeepMemorySize() {
        long size = 0L;
        for(TimeSeriesSpan span : intervalTree){
            size += span.calculateDeepMemorySize();
        }
        return size;
    }

}
