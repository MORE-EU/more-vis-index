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

    private int[] ioCount = {0};

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

    private List<TimeSeriesSpan> getOverlappingIntervals(long from, long to, List<Integer> measures,
                                                         AggregateInterval m4Interval, AggregateInterval subInterval,
                                                         ImmutableRangeSet<Long>[] currentDifference){
        RangeSet<Long> rangeSet = TreeRangeSet.create();
        final int[] ioCount = {0};
        TimeRange timeRange = new TimeRange(from, to);
        // Sort overlapping spans, by their query coverage. Then find which are the ones covering the whole range, and
        // also keep the remaining difference.
        List<TimeSeriesSpan> overlappingIntervals = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(intervalTree.overlappers(timeRange), 0), false)
                .filter(span -> span.getAggregateInterval().toDuration()
                        .compareTo(m4Interval.toDuration()) <= 0 && (span.overlaps(timeRange)))
                .sorted(Comparator.comparing(span -> span.percentage(timeRange), Comparator.reverseOrder()))
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

        return overlappingIntervals;
    }


    @SuppressWarnings("UnstableApiUsage")
    public List<TimeSeriesSpan> getTimeSeriesSpans(long from, long to, List<Integer> measures,
                                         AggregateInterval m4Interval, AggregateInterval subInterval,
                                         ImmutableRangeSet<Long>[] currentDifference){
        List<TimeSeriesSpan> overlappingIntervals = getOverlappingIntervals(from, to, measures, m4Interval, subInterval, currentDifference);
        // Calculate and add missing intervals
        List<TimeRange> ranges = currentDifference[0].asRanges().stream()
                .map(r -> new TimeRange(r.lowerEndpoint(), r.upperEndpoint())).collect(Collectors.toList());
        if(ranges.size() >= 1) {
            long newFrom = Math.max(dataset.getTimeRange().getFrom(), (from - (to - from) / 2));
            long newTo = Math.min(dataset.getTimeRange().getTo(), (to + (to - from) / 2));
            currentDifference = new ImmutableRangeSet[]{ImmutableRangeSet.of(Range.closed(newFrom, newTo))};
            overlappingIntervals = getOverlappingIntervals(newFrom, newTo, measures, m4Interval, subInterval, currentDifference);
            ranges = currentDifference[0].asRanges().stream()
                    .map(r -> new TimeRange(r.lowerEndpoint(), r.upperEndpoint())).collect(Collectors.toList());

            AggregatedDataPoints dataPoints =
                    dataSource.getAggregatedDataPoints(newFrom, newTo, ranges, measures, subInterval);
            List<TimeSeriesSpan> timeSeriesSpans = TimeSeriesSpanFactory.create(dataPoints, ranges, subInterval);
            overlappingIntervals.addAll(timeSeriesSpans);
            intervalTree.insertAll(timeSeriesSpans);
            timeSeriesSpans.forEach(s -> ioCount[0] += s.getCounts()[0]);
        }
        overlappingIntervals.sort((i1, i2) -> (int) (i1.getFrom() - i2.getFrom())); // Sort intervals
        return overlappingIntervals;
    }

    public QueryResults executeQuery(AbstractQuery query) {
        LOG.info("Executing query: " + query.getFromDate() + " - " + query.getToDate());
        ioCount = new int[]{0};
        Duration m4Duration = DateTimeUtil.M4(query.getFrom(), query.getTo(), query.getViewPort());
        AggregateInterval m4Interval = DateTimeUtil.aggregateCalendarInterval(m4Duration);
        Duration subDuration = DateTimeUtil.accurateCalendarInterval(query.getFrom(), query.getTo(), query.getViewPort(), accuracy);
        subDuration = subDuration.toMillis() < dataset.getSamplingInterval()
                .toMillis() ? dataset.getSamplingInterval() : subDuration;
        AggregateInterval subInterval = DateTimeUtil.aggregateCalendarInterval(subDuration);
        LOG.info("Interval: " + subInterval);

        QueryResults queryResults = new QueryResults();
        List<Integer> measures = query.getMeasures() == null ? dataset.getMeasures() : query.getMeasures();
        long from = query.getFrom();
        long to = query.getTo();
        ImmutableRangeSet<Long>[] currentDifference = new ImmutableRangeSet[]{ImmutableRangeSet.of(Range.closed(from, to))};
        List<TimeSeriesSpan> overlappingIntervals = getTimeSeriesSpans(from, to, measures, m4Interval, subInterval, currentDifference);

        GroupByEvaluator groupByEvaluator = query.getGroupByField() != null
                ? new GroupByEvaluator(measures, query.getGroupByField())
                : null;
        MultiSpanIterator<TimeSeriesSpan> multiSpanIterator = new MultiSpanIterator(overlappingIntervals.iterator(), groupByEvaluator);
        PixelAggregator pixelAggregator = new PixelAggregator(multiSpanIterator, query.getFrom(), query.getTo(), measures, m4Interval, query.getViewPort());
        Map<Integer, List<UnivariateDataPoint>> data = measures.stream()
                .collect(Collectors.toMap(Function.identity(), ArrayList::new));

        while (pixelAggregator.hasNext()) {
            PixelAggregatedDataPoint next = pixelAggregator.next();
            PixelStatsAggregator stats = (PixelStatsAggregator) next.getStats();
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
