package eu.more2020.visual.index;

import com.google.common.base.Stopwatch;
import eu.more2020.visual.datasource.DataSource;
import eu.more2020.visual.datasource.DataSourceFactory;
import eu.more2020.visual.domain.*;
import eu.more2020.visual.domain.Dataset.AbstractDataset;
import eu.more2020.visual.domain.Query.Query;
import eu.more2020.visual.util.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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

    class SpanIteratorPair {
        TimeSeriesSpan span;
        AggregatedDataPoint aggregatedDataPoint;
        Iterator<AggregatedDataPoint> iterator;

        SpanIteratorPair(TimeSeriesSpan span, AggregatedDataPoint aggregatedDataPoint, Iterator<AggregatedDataPoint> iterator) {
            this.span = span;
            this.aggregatedDataPoint = aggregatedDataPoint;
            this.iterator = iterator;
        }
    }

    public QueryResults executeQuery(Query query) {
        long from = query.getFrom();
        long to = query.getTo();
        QueryResults queryResults = new QueryResults();
        ViewPort viewPort = query.getViewPort();
        long pixelColumnInterval = (to - from) / viewPort.getWidth();
        LOG.debug("Pixel column interval: " + pixelColumnInterval + " ms");
        List<Integer> measures = Optional.ofNullable(query.getMeasures()).orElse(dataset.getMeasures());

        List<PixelColumn> pixelColumns = new ArrayList<>();
        for (long i = 0; i < viewPort.getWidth(); i++) {
            long pixelFrom = from + (i * pixelColumnInterval);
            long pixelTo = pixelFrom + pixelColumnInterval;
            PixelColumn pixelColumn = new PixelColumn(pixelFrom, pixelTo, measures, viewPort);
            pixelColumns.add(pixelColumn);
        }
        LOG.debug("Created {} pixel columns: {}", viewPort.getWidth(), pixelColumns.stream().map(PixelColumn::getIntervalString).collect(Collectors.joining(", ")));


        // Query the interval tree for all spans that overlap the query interval.
        List<TimeSeriesSpan> overlappingSpans = StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(intervalTree.overlappers(query), 0), false)
                // Filter out spans with aggregate interval larger than the pixel column interval.
                // This way, each of the groups of the resulting spans will overlap at most two pixel columns.
                .filter(span -> span.getAggregateInterval() <= pixelColumnInterval)
                .collect(Collectors.toList());




        // Create a priority queue storing the SpanIteratorPair, and
        // populate it with the first data point from each span's iterator.
        PriorityQueue<SpanIteratorPair> queue = new PriorityQueue<>(
                Comparator.comparing((SpanIteratorPair pair) -> pair.aggregatedDataPoint.getTimestamp())
                        .thenComparingLong(pair -> pair.span.getAggregateInterval()));

        for (TimeSeriesSpan span : overlappingSpans) {
            Iterator<AggregatedDataPoint> iterator = span.iterator(from, to);
            if (iterator.hasNext()) {
                queue.add(new SpanIteratorPair(span, iterator.next(), iterator));
            }
        }

        // currentTime is the end timestamp of the last data point that was processed.
        long currentTime = -1;
        TimeSeriesSpan lastSpanUsed = null;

        // Repeatedly pull from the queue, getting the earliest next data point from the
        // span with the smallest aggregation interval.
        while (!queue.isEmpty()) {
            SpanIteratorPair pair = queue.poll();
            Iterator<AggregatedDataPoint> iterator = pair.iterator;
            AggregatedDataPoint aggregatedDataPoint = pair.aggregatedDataPoint;
            TimeSeriesSpan span = pair.span;

//            LOG.debug("Processing data point with timestamp: " + aggregatedDataPoint.getTimestamp() + ", aggregation interval: " + aggregatedDataPoint.getAggregateInterval() + ", and count: " + stats.getCount());

//            while (pair.aggregatedDataPoint.getTimestamp() > currentTime) {
//
//            }

/*            if (lastSpanUsed != null && aggregatedDataPoint.getTimestamp() < currentTime && span.getAggregateInterval() < lastSpanUsed.getAggregateInterval()) {
                // If the data point is before currentTime, and the aggregation interval is greater than or equal to the last span used,
                // then we can skip this data point and continue to the next one.
                LOG.debug("Skipping data point with timestamp: " + aggregatedDataPoint.getTimestamp() + " and aggregation interval: " + span.getAggregateInterval());
                continue;
            }*/


            addAggregatedDataPointToPixelColumns(query, pixelColumns, aggregatedDataPoint, viewPort);

            // Update currentTime to the end of the current data point's covered time
            currentTime = pair.aggregatedDataPoint.getTo();

            // Update lastSpanUsed to the span that was just used
            lastSpanUsed = span;

            // If the iterator has more data points, add the next one to the queue.
            if (iterator.hasNext()) {
                queue.add(new SpanIteratorPair(span, iterator.next(), iterator));
            }
        }

//        MaxErrorEvaluator maxErrorEvaluator = new MaxErrorEvaluator(measures, viewPort, pixelColumns);
//        LOG.debug("Errors: " + maxErrorEvaluator.computeMaxError());


        // Find the part of the query interval that is not covered by the spans in the interval tree.
        List<TimeInterval> missingIntervals = query.difference(overlappingSpans);
        LOG.info("Missing from query: " + missingIntervals);
        double queryTime = 0;

        // Fetch the missing data from the data source.
        List<AggregatedDataPoint> missingDataPointList = null;
        AggregatedDataPoints missingDataPoints = null;
        if (missingIntervals.size() >= 1) {
            LOG.info("Fetching missing data from data source");
            Stopwatch stopwatch = Stopwatch.createStarted();
            missingIntervals = DateTimeUtil.correctIntervals(from, to, viewPort.getWidth(), missingIntervals);
            missingDataPoints = dataSource.getAggregatedDataPoints(from, to, missingIntervals, measures,  2 * viewPort.getWidth());
            missingDataPointList = StreamSupport.stream(missingDataPoints.spliterator(), false).collect(Collectors.toList());

            queryTime = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
            stopwatch.stop();
            LOG.info("Fetched missing data from data source");

            // Add the data points fetched from the data store to the pixel columns
            missingDataPointList.forEach(aggregatedDataPoint -> {
                addAggregatedDataPointToPixelColumns(query, pixelColumns, aggregatedDataPoint, viewPort);
            });

            LOG.debug("Added fetched data points to pixel columns");

            // Create time series spans from the missing data and insert them into the interval tree.
            List<TimeSeriesSpan> timeSeriesSpans = TimeSeriesSpanFactory.create(missingDataPointList, measures, missingIntervals, 2 * viewPort.getWidth());

            timeSeriesSpans.forEach(t -> queryResults.setIoCount(queryResults.getIoCount() + Arrays.stream(t.getCounts()).sum()));
            timeSeriesSpans.forEach(t -> t.iterator().forEachRemaining(dp -> {
                AggregatedDataPoint aggregatedDataPoint = (AggregatedDataPoint) dp;
                LOG.debug("Span data point: {} with stats {}", aggregatedDataPoint.getIntervalString(), aggregatedDataPoint.getStats().getString(2));
            }));

            intervalTree.insertAll(timeSeriesSpans);
            overlappingSpans.addAll(timeSeriesSpans);
            LOG.info("Inserted new time series spans into interval tree");
        }








/*        // If currentTime is still less than the end of the query interval, then we need to fetch more data from the data source.
        while (currentTime < to) {
        }*/


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
        Map<Integer, List<UnivariateDataPoint>> resultData = new HashMap<>();
        for (int measure : measures) {
            List<UnivariateDataPoint> dataPoints = new ArrayList<>();
            for (PixelColumn pixelColumn : pixelColumns) {
                Stats pixelColumnStats = pixelColumn.getStats();
                if (pixelColumnStats.getCount() == 0) {
                    continue;
                }
                dataPoints.add(new UnivariateDataPoint(pixelColumnStats.getFirstTimestamp(measure), pixelColumnStats.getFirstValue(measure)));
                dataPoints.add(new UnivariateDataPoint(pixelColumnStats.getMinTimestamp(measure), pixelColumnStats.getMinValue(measure)));
                dataPoints.add(new UnivariateDataPoint(pixelColumnStats.getMaxTimestamp(measure), pixelColumnStats.getMaxValue(measure)));
                dataPoints.add(new UnivariateDataPoint(pixelColumnStats.getLastTimestamp(measure), pixelColumnStats.getLastValue(measure)));
            }
            resultData.put(measure, dataPoints);
        }
        //todo: remove this, just for debugging here
        resultData.forEach((k, v) -> v.sort(Comparator.comparingLong(UnivariateDataPoint::getTimestamp)));
        queryResults.setData(resultData);

        Map<Integer, Double> error = new HashMap<>();
        for (
                Integer measure : measures) {
            error.put(measure, 0d);
        }
        queryResults.setError(error);
        queryResults.setQueryTime(queryTime);
        return queryResults;
    }


    /*public QueryResults executeQuery(AbstractQuery query) {
        long from = query.getFrom();
        long to = query.getTo();
        QueryResults queryResults = new QueryResults();
        ViewPort viewPort = query.getViewPort();

        List<Integer> measures = Optional.ofNullable(query.getMeasures()).orElse(dataset.getMeasures());



        List<PixelColumn> pixelColumns = createPixelColumns(from, to, viewPort.getWidth());
        LOG.debug("Created pixel columns");

        List<TimeSeriesSpan> overlappingSpans = StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(intervalTree.overlappers(query), 0), false)
                .collect(Collectors.toList());

        AggregateInterval pixelColumnInterval = DateTimeUtil.M4Interval(from, to,  viewPort.getWidth());


        List<TimeInterval> missingIntervals = query.difference(overlappingSpans);
        LOG.info("Missing from query: " + missingIntervals);

        if (missingIntervals.size() >= 1) {
            LOG.info("Fetching missing data from data source");
            AggregatedDataPoints dataPoints =
                    dataSource.getAggregatedDataPoints(from, to, missingIntervals, measures, pixelColumnInterval);
            LOG.info("Fetched missing data from data source");
            List<TimeSeriesSpan> timeSeriesSpans = TimeSeriesSpanFactory.create(dataPoints, missingIntervals, pixelColumnInterval);
            LOG.info("Created time series spans for fetched data");
            timeSeriesSpans.forEach(t -> queryResults.setIoCount(queryResults.getIoCount() + Arrays.stream(t.getCounts()).sum()));
            intervalTree.insertAll(timeSeriesSpans);
            overlappingSpans.addAll(timeSeriesSpans);
            LOG.info("Inserted new time series spans into interval tree");
        }



        for (TimeSeriesSpan span : overlappingSpans) {
            LOG.debug("Processing time series span: " + span);
            for (int measure : measures) {
                Iterator<UnivariateDataPoint> iterator = span.getMinMaxIterator(from, to, measure);
                while (iterator.hasNext()) {
                    UnivariateDataPoint point = iterator.next();
                    PixelColumn pixelColumn = pixelColumns.get(getPixelColumnForTimestamp(point.getTimestamp(), from, to, viewPort.getWidth()));
                    pixelColumn.addDataPoint(point, measure);
                }
            }
        }



        *//*boolean reEvaluate = false;
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
            }*//*
        Map<Integer, List<UnivariateDataPoint>> resultData = new HashMap<>();
        for (int measure : measures) {
            List<UnivariateDataPoint> dataPoints = new ArrayList<>();
            for (PixelColumn pixelColumn : pixelColumns) {
                UnivariateDataPoint firstPoint = pixelColumn.getFirstPoints().get(measure);
                UnivariateDataPoint minPoint = pixelColumn.getMinPoints().get(measure);
                UnivariateDataPoint maxPoint = pixelColumn.getMaxPoints().get(measure);
                UnivariateDataPoint lastPoint = pixelColumn.getLastPoints().get(measure);
                if (firstPoint != null) {
                    dataPoints.add(firstPoint);
                    if (minPoint.getTimestamp() < maxPoint.getTimestamp()) {
                        dataPoints.add(minPoint);
                        dataPoints.add(maxPoint);
                    } else {
                        dataPoints.add(maxPoint);
                        dataPoints.add(minPoint);
                    }
                    dataPoints.add(lastPoint);
                }
            }
            resultData.put(measure, dataPoints);
        }
        queryResults.setData(resultData);

        Map<Integer, Double> error = new HashMap<>();
        for (Integer measure : measures) {
            error.put(measure, 0d);
        }
        queryResults.setError(error);
        return queryResults;
    }*/


    private int getPixelColumnForTimestamp(long timestamp, long from, long to, int width) {
/*        if (timestamp == to) {
            return width - 1;
        } else {*/
        return (int) ((double) width * (timestamp - from) / (to - from));
    }

    private void addAggregatedDataPointToPixelColumns(Query query, List<PixelColumn> pixelColumns, AggregatedDataPoint aggregatedDataPoint, ViewPort viewPort) {
        int pixelColumnIndex = getPixelColumnForTimestamp(aggregatedDataPoint.getFrom(), query.getFrom(), query.getTo(), viewPort.getWidth());

        if (pixelColumnIndex < viewPort.getWidth()) {
            pixelColumns.get(pixelColumnIndex).addAggregatedDataPoint(aggregatedDataPoint);
        }
        // Since we only consider spans with intervals smaller than the pixel column interval, we know that the data point will not overlap more than two pixel columns.
        if (pixelColumnIndex < viewPort.getWidth() - 1 && pixelColumns.get(pixelColumnIndex + 1).overlaps(aggregatedDataPoint)) {
            // If the next pixel column overlaps the data point, then we need to add the data point to the next pixel column as well.
            pixelColumns.get(pixelColumnIndex + 1).addAggregatedDataPoint(aggregatedDataPoint);
        }
    }


/*    private List<TimeSeriesSpan> getOverlappingSpans(AbstractQuery query, AggregateInterval pixelColumnInterval) {
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
    }*/

/*
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
*/

/*
    public Map<Integer, Double> getError(PixelAggregator pixelAggregator, List<Integer> measures) {
        Map<Integer, Double> error = new HashMap<>(measures.size());
        int i = 0;
        for (Integer measure : measures) {
            error.put(measure, pixelAggregator.getError(measure));
            LOG.info("Query Max Error (" + measure + "): " + Double.parseDouble(String.format("%.3f", pixelAggregator.getError(measure) * 100)) + "%");
            i++;
        }
        return error;
    }*/

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
