package eu.more2020.visual.index;

import com.google.common.collect.ImmutableRangeSet;
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

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class TTI {

    private static final Logger LOG = LoggerFactory.getLogger(TTI.class);

    private final AbstractDataset dataset;

    private final DataSource dataSource;


    Comparator<UnivariateDataPoint> compareLists = (s1, s2) -> {
        if (s1 == null && s2 == null) return 0; //swapping has no point here
        if (s1 == null) return 1;
        if (s2 == null) return -1;
        return (int) (Long.compare(s1.getTimestamp(), s2.getTimestamp()));
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

    public QueryResults executeQuery(AbstractQuery query) {
        long from = query.getFrom();
        long to = query.getTo();
        float accuracy = query.getAccuracy();
        ViewPort viewPort = query.getViewPort();
        AggregateInterval pixelColumnInterval = DateTimeUtil.M4Interval(from, to, viewPort);
        List<Integer> measures = Optional.ofNullable(query.getMeasures()).orElse(dataset.getMeasures());

        QueryResults queryResults = new QueryResults();


        List<TimeSeriesSpan> overlappingSpans = getOverlappingSpans(query, pixelColumnInterval);
        List<TimeInterval> missingIntervals = query.difference(overlappingSpans);

        LOG.info("Missing from query: " + missingIntervals);

        if (missingIntervals.size() >= 1) {
            AggregatedDataPoints dataPoints =
                    dataSource.getAggregatedDataPoints(from, to, missingIntervals, measures, pixelColumnInterval);
            List<TimeSeriesSpan> timeSeriesSpans = TimeSeriesSpanFactory.create(dataPoints, missingIntervals, pixelColumnInterval);
            timeSeriesSpans.forEach(t -> queryResults.setIoCount(queryResults.getIoCount() + Arrays.stream(t.getCounts()).sum()));
            intervalTree.insertAll(timeSeriesSpans);
            overlappingSpans.addAll(timeSeriesSpans);
            overlappingSpans.sort(Comparator.comparing(TimeSeriesSpan::getFrom)); // Sort intervals
        }

        MultiSpanIterator<TimeSeriesSpan> multiSpanIterator = new MultiSpanIterator(overlappingSpans.iterator());


        PixelAggregator pixelAggregator = new PixelAggregator(multiSpanIterator, from, to, measures, pixelColumnInterval, viewPort);
        Map<Integer, List<UnivariateDataPoint>> data = getData(pixelAggregator, measures);
        Map<Integer, Double> error = getError(pixelAggregator, measures);
        /*boolean reEvaluate = false;
            for (double v : error.values()) {
                if (v > 0.05) {
                    reEvaluate = true;
                    break;
                }
            }
            if (reEvaluate) {
                missingIntervals = new ArrayList<>();
                missingIntervals.add(new TimeRange(from, to));
                subInterval = new AggregateInterval((subInterval.getInterval() / 2), subInterval.getChronoUnit());
                AggregatedDataPoints dataPoints =
                        dataSource.getAggregatedDataPoints(from, to, missingIntervals, measures, subInterval);
                timeSeriesSpans = TimeSeriesSpanFactory.create(dataPoints, ttiQueryResults.getMissingIntervals(), subInterval);
                timeSeriesSpans.forEach(t -> ioCount[0] += (Arrays.stream(t.getCounts()).sum()));
                intervalTree.insertAll(timeSeriesSpans);
                continue;
            }*/
        queryResults.setData(data);
        queryResults.setError(error);
        return queryResults;
    }


    private List<TimeSeriesSpan> getOverlappingSpans(AbstractQuery query, AggregateInterval pixelColumnInterval) {
        ImmutableRangeSet<Long>[] currentDifference = new ImmutableRangeSet[]{ImmutableRangeSet.of(TimeInterval.toGuavaRange(query))};
        RangeSet<Long> rangeSet = TreeRangeSet.create();
        // Sort overlapping spans, by their query coverage. Then find which are the ones covering the whole range, and
        // also keep the remaining difference.
        List<TimeSeriesSpan> overlappingSpans = StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(intervalTree.overlappers(query), 0), false)
                .filter(span -> span.getAggregateInterval().toDuration().compareTo(pixelColumnInterval.toDuration()) <= 0)
                // todo: why not sort by agg interval of every span?
                .sorted(Comparator.comparing(span -> span.percentage(query), Comparator.reverseOrder()))
                .filter(span -> {
                    if (currentDifference[0].isEmpty())
                        return false; // If the difference has been covered, don't check.
                    rangeSet.add(TimeInterval.toGuavaRange(span));
                    ImmutableRangeSet<Long> newDifference = currentDifference[0].difference(rangeSet);
                    if (!currentDifference[0].equals(newDifference)) { // If the current span, added to the difference, keep it.
                        currentDifference[0] = newDifference;
                        return true;
                    }
                    return false;
                })
                .collect(Collectors.toList());

        overlappingSpans.sort((i1, i2) -> (int) (i1.getFrom() - i2.getFrom())); // Sort intervals
        return overlappingSpans;
    }

    public Map<Integer, List<UnivariateDataPoint>> getData(PixelAggregator pixelAggregator, List<Integer> measures) {
        Map<Integer, List<UnivariateDataPoint>> data = measures.stream()
                .collect(Collectors.toMap(Function.identity(), ArrayList::new));
        while (pixelAggregator.hasNext()) {
            PixelAggregatedDataPoint next = pixelAggregator.next();

            PixelStatsAggregator stats = (PixelStatsAggregator) next.getStats();
            if (stats.getCount() != 0) {
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


    public Map<Integer, Double> getError(PixelAggregator pixelAggregator, List<Integer> measures) {
        Map<Integer, Double> error = new HashMap<>(measures.size());
        int i = 0;
        for (Integer measure : measures) {
            error.put(measure, pixelAggregator.getError(measure));
            LOG.info("Query Max Error (" + measure + "): " + Double.parseDouble(String.format("%.3f", pixelAggregator.getError(measure) * 100)) + "%");
            i++;
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
        for (TimeSeriesSpan span : intervalTree) {
            size += span.calculateDeepMemorySize();
        }
        return size;
    }

}
