package eu.more2020.visual.index;

import com.beust.ah.A;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import eu.more2020.visual.datasource.DataSource;
import eu.more2020.visual.datasource.DataSourceFactory;
import eu.more2020.visual.domain.*;
import eu.more2020.visual.domain.Dataset.AbstractDataset;
import eu.more2020.visual.domain.Query.AbstractQuery;
import eu.more2020.visual.util.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Time;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TTI {

    private static final Logger LOG = LoggerFactory.getLogger(TTI.class);

    private final AbstractDataset dataset;

    private final DataSource dataSource;

    private int[] ioCount = {0};

    Comparator<UnivariateDataPoint> compareLists = new Comparator<UnivariateDataPoint>() {
        @Override
        public int compare(UnivariateDataPoint s1, UnivariateDataPoint s2) {
            if (s1 == null && s2 == null) return 0; //swapping has no point here
            if (s1 == null) return 1;
            if (s2 == null) return -1;
            return (int) (Long.compare(s1.getTimestamp(), s2.getTimestamp()));
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


    public QueryResults evaluate(long from, long to, float accuracy,
                                                            List<Integer> measures,
                                                            ViewPort viewPort,
                                                            AggregateInterval m4Interval){
        QueryResults queryResults = new QueryResults();
//        AggregateInterval subInterval = DateTimeUtil.accurateInterval(from, to, viewPort, dataset.getSamplingInterval(), accuracy);
        TTIQueryResults ttiQueryResults = getIntervals(from, to, m4Interval);
        List<TimeSeriesSpan> timeSeriesSpans = ttiQueryResults.getOverlappingIntervals();
        List<TimeRange> missingIntervals = ttiQueryResults.getMissingIntervals();
        LOG.info("Missing from query: " + missingIntervals);
        if(missingIntervals.size() >= 1) {
            calculateMissingIntervals(ttiQueryResults, measures, m4Interval);
        }
        LOG.info(String.valueOf(ttiQueryResults.getOverlappingIntervals()));
        while(true) {
            MultiSpanIterator<TimeSeriesSpan> multiSpanIterator = new MultiSpanIterator(timeSeriesSpans.iterator());

            PixelAggregator pixelAggregator = new PixelAggregator(multiSpanIterator, from, to, measures, m4Interval, viewPort);
            Map<Integer, List<UnivariateDataPoint>> data = getData(pixelAggregator, measures);
            Map<Integer, Double> error = getError(pixelAggregator, measures);
            boolean reEvaluate = false;
            for (double v : error.values()) {
                if (v > 0.05) {
                    reEvaluate = true;
                    break;
                }
            }
            if(reEvaluate){
                missingIntervals = new ArrayList<>();
                missingIntervals.add(new TimeRange(from, to));
                AggregateInterval interval = new AggregateInterval((m4Interval.getInterval() /  2), m4Interval.getChronoUnit());
                AggregatedDataPoints dataPoints =
                        dataSource.getAggregatedDataPoints(from, to, missingIntervals, measures, interval);
                timeSeriesSpans = TimeSeriesSpanFactory.create(dataPoints, ttiQueryResults.getMissingIntervals(), interval);
                timeSeriesSpans.forEach(t -> ioCount[0] += (Arrays.stream(t.getCounts()).sum()));
                intervalTree.insertAll(timeSeriesSpans);
                continue;
            }
            queryResults.setData(data);
            queryResults.setIoCount(ioCount[0]);
            queryResults.setError(error);
            return queryResults;
        }
    }

    public QueryResults executeQuery(AbstractQuery query) {
        ioCount = new int[]{0};
        long from = query.getFrom();
        long to = query.getTo();
        float accuracy = query.getAccuracy();
        ViewPort viewPort = query.getViewPort();
        AggregateInterval m4Interval = DateTimeUtil.M4Interval(from, to, viewPort);
        List<Integer> measures = query.getMeasures() == null ? dataset.getMeasures() : query.getMeasures();

        return evaluate(from, to, accuracy, measures, viewPort, m4Interval);
    }


    public TTIQueryResults calculateMissingIntervals(TTIQueryResults ttiQueryResults, List<Integer> measures, AggregateInterval interval){
//        long newFrom = Math.max(dataset.getTimeRange().getFrom(), (from - (to - from) / 2));
//        long newTo = Math.min(dataset.getTimeRange().getTo(), (to + (to - from) / 2));
//        LOG.info("Prefetching query: " + new TimeRange(newFrom, newTo));
//        LOG.info("M4: " + m4Interval);
//        LOG.info("Sub: " + subInterval);
        long from = ttiQueryResults.getFrom();
        long to = ttiQueryResults.getTo();
        // Calculate and add missing intervals
        AggregatedDataPoints dataPoints =
                dataSource.getAggregatedDataPoints(from, to, ttiQueryResults.getMissingIntervals(), measures, interval);
        List<TimeSeriesSpan> timeSeriesSpans = TimeSeriesSpanFactory.create(dataPoints, ttiQueryResults.getMissingIntervals(), interval);
        timeSeriesSpans.forEach(t -> ioCount[0] += (Arrays.stream(t.getCounts()).sum()));
        intervalTree.insertAll(timeSeriesSpans);
        ttiQueryResults.addAll(timeSeriesSpans);
        return ttiQueryResults;
    }

    private TTIQueryResults getIntervals(long from, long to, AggregateInterval interval){
        ImmutableRangeSet<Long>[] currentDifference = new ImmutableRangeSet[]{ImmutableRangeSet.of(Range.closed(from, to))};
        RangeSet<Long> rangeSet = TreeRangeSet.create();
        TimeRange timeRange = new TimeRange(from, to);
        // Sort overlapping spans, by their query coverage. Then find which are the ones covering the whole range, and
        // also keep the remaining difference.
        List<TimeSeriesSpan> overlappingIntervals = StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(intervalTree.overlappers(timeRange), 0), false)
                .filter(span -> span.getAggregateInterval().toDuration()
                        .compareTo(interval.toDuration()) <= 0 && (span.overlaps(timeRange)))
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
        List<TimeRange> ranges = currentDifference[0].asRanges().stream()
                .map(r -> new TimeRange(r.lowerEndpoint(), r.upperEndpoint())).collect(Collectors.toList());
        return new TTIQueryResults(from, to, overlappingIntervals, ranges);
    }

    public Map<Integer, List<UnivariateDataPoint>> getData(PixelAggregator pixelAggregator, List<Integer> measures){
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
        data.forEach((k, v) -> v.sort(compareLists));
        return data;
    }


    public Map<Integer, Double> getError(PixelAggregator pixelAggregator, List<Integer> measures){
        Map<Integer, Double> error = new HashMap<>(measures.size());
        int i = 0;
        for (Integer measure : measures) {
            error.put(measure, pixelAggregator.getError(measure));
            LOG.info("Query Max Error (" + measure  +"): " + Double.parseDouble(String.format("%.3f", pixelAggregator.getError(measure) * 100)) + "%");
            i ++;
        }
        return error;
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
