package eu.more2020.visual.middleware.cache;

import com.google.common.base.Stopwatch;

import eu.more2020.visual.middleware.domain.*;
import eu.more2020.visual.middleware.domain.Dataset.AbstractDataset;
import eu.more2020.visual.middleware.domain.Query.Query;
import eu.more2020.visual.middleware.domain.Query.QueryMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CacheQueryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(MinMaxCache.class);
    private final AbstractDataset dataset;
    private int aggFactor;

    public CacheQueryExecutor(AbstractDataset dataset, int aggFactor) {
        this.dataset = dataset;
        this.aggFactor = aggFactor;
    }

    void updateAggFactor(){
        aggFactor *= 2;
    }

    public QueryResults executeQuery(Query query, CacheManager cacheManager,
                                     ErrorCalculator errorCalculator, DataProcessor dataProcessor, PrefetchManager prefetchManager){
        LOG.info("Executing Visual Query {}", query);
        long from = query.getFrom();
        long to = query.getTo();

        QueryResults queryResults = new QueryResults();
        ViewPort viewPort = query.getViewPort();

        long pixelColumnInterval = (to - from) / viewPort.getWidth();
        double queryTime = 0;
        boolean gotData = false;

        Stopwatch stopwatch = Stopwatch.createUnstarted();
        stopwatch.start();
        LOG.debug("Pixel column interval: " + pixelColumnInterval + " ms");
        List<Integer> measures = Optional.ofNullable(query.getMeasures()).orElse(dataset.getMeasures());

        List<PixelColumn> pixelColumns = new ArrayList<>();
        for (long i = 0; i < viewPort.getWidth(); i++) {
            long pixelFrom = from + (i * pixelColumnInterval);
            long pixelTo = pixelFrom + pixelColumnInterval;
            PixelColumn pixelColumn = new PixelColumn(pixelFrom, pixelTo, measures, viewPort);
            pixelColumns.add(pixelColumn);
        }
        List<TimeSeriesSpan> overlappingSpans = cacheManager.getFromCache(query, pixelColumnInterval);
        //LOG.debug("Created {} pixel columns: {}", viewPort.getWidth(), pixelColumns.stream().map(PixelColumn::getIntervalString).collect(Collectors.joining(", ")));

        LOG.debug("Overlapping intervals {}", overlappingSpans.stream().map(span -> span.getAggregateInterval() + " (" + span.getMeasures() + ")").collect(Collectors.joining(", ")));

        dataProcessor.processDatapoints(from, to, query, viewPort, pixelColumns, overlappingSpans);
        errorCalculator.calculateValidColumnsErrors(query, pixelColumns, viewPort, pixelColumnInterval);
        List<MultivariateTimeInterval> missingMultiIntervals = errorCalculator.getMissingIntervals();
        List<TimeInterval> missingIntervals = missingMultiIntervals.stream().map(MultivariateTimeInterval::getInterval).collect(Collectors.toList());
        List<List<Integer>> missingMeasures = missingMultiIntervals.stream().map(MultivariateTimeInterval::getMeasures).collect(Collectors.toList());
        LOG.info(String.valueOf(missingMultiIntervals));
        if(!missingIntervals.isEmpty()) gotData = true;
        double highestScore = Double.MIN_VALUE;
        double highestCoverage = Double.MIN_VALUE;
        for (TimeSeriesSpan overlappingSpan : overlappingSpans) {
            long size = overlappingSpan.getAggregateInterval();
            double coveragePercentage = overlappingSpan.percentage(query);
            double score = size * coveragePercentage;
            if (score > highestScore) {
                highestScore = score;
                highestCoverage = coveragePercentage;
                aggFactor = (int) (pixelColumnInterval / size);
            }
        }
//        aggFactor = Math.min(aggFactor, 10);
        if(errorCalculator.hasError()){
            updateAggFactor();
        }
        LOG.debug("Agg factor = {}", aggFactor);
        // Fetch the missing data from the data source.
        List<TimeSeriesSpan> missingTimeSeriesSpans =
                dataProcessor.getMissingAndAddToPixelColumns(from, to, missingMeasures, viewPort, missingIntervals, query, queryResults, aggFactor, pixelColumns);
        // Add them all to the cache.
        cacheManager.addToCache(missingTimeSeriesSpans);

        // Recalculate error
        Map<Integer, Double> finalError  = errorCalculator.calculateTotalError(query, pixelColumns, viewPort, pixelColumnInterval);
        if(errorCalculator.hasError()) {
            pixelColumns = new ArrayList<>();
            for (long i = 0; i < viewPort.getWidth(); i++) {
                long pixelFrom = from + (i * pixelColumnInterval);
                long pixelTo = pixelFrom + pixelColumnInterval;
                PixelColumn pixelColumn = new PixelColumn(pixelFrom, pixelTo, measures, viewPort);
                pixelColumns.add(pixelColumn);
            }
            gotData = true;
            // Initialize ranges and measures to get all errored data.
            missingIntervals = new ArrayList<>();
            missingIntervals.add(new TimeRange(from, to));
            missingMeasures = new ArrayList<>();
            missingMeasures.add(measures);

            LOG.info("Cached data are above error bound. Fetching {}: ", missingIntervals);
            LOG.info("Fetching missing data from data source");
            query.setQueryMethod(QueryMethod.M4);
            long timeStart = System.currentTimeMillis();
            missingTimeSeriesSpans =
                    dataProcessor.getMissingAndAddToPixelColumns(from, to, missingMeasures, viewPort, missingIntervals, query, queryResults, 1, pixelColumns);
            cacheManager.addToCache(missingTimeSeriesSpans);
            queryResults.setProgressiveQueryTime((System.currentTimeMillis() - timeStart) / 1000F);
            measures.forEach(m -> finalError.put(m, 0.0)); // set max error to 0
        }

        // Query Results
        Map<Integer, List<UnivariateDataPoint>> resultData = new HashMap<>();
        Map<Integer, DoubleSummaryStatistics> measureStatsMap = new HashMap<>();
        for (int measure : measures) {
            int count = 0;
            double max = Double.MIN_VALUE;
            double min = Double.MAX_VALUE;
            double sum = 0;
            List<UnivariateDataPoint> dataPoints = new ArrayList<>();

            for (PixelColumn pixelColumn : pixelColumns) {
                Stats pixelColumnStats = pixelColumn.getStats();
                if (pixelColumnStats.getCount(measure) <= 0) {
                    continue;
                }
                // filter
                if(query.getFilter() == null || query.getFilter().isEmpty()){
                    dataPoints.add(new UnivariateDataPoint(pixelColumnStats.getFirstTimestamp(measure), pixelColumnStats.getFirstValue(measure)));
                    dataPoints.add(new UnivariateDataPoint(pixelColumnStats.getMinTimestamp(measure), pixelColumnStats.getMinValue(measure)));
                    dataPoints.add(new UnivariateDataPoint(pixelColumnStats.getMaxTimestamp(measure), pixelColumnStats.getMaxValue(measure)));
                    dataPoints.add(new UnivariateDataPoint(pixelColumnStats.getLastTimestamp(measure), pixelColumnStats.getLastValue(measure)));
                }
                else {
                    double filterMin = query.getFilter().get(measure)[0];
                    double filterMax = query.getFilter().get(measure)[1];
                    LOG.debug("{}, {}", filterMin, filterMax);
                    if (filterMin < pixelColumnStats.getMinValue(measure) &&
                            filterMax > pixelColumnStats.getMaxValue(measure)) {
                        LOG.info("Added datapoint");
                        dataPoints.add(new UnivariateDataPoint(pixelColumnStats.getFirstTimestamp(measure), pixelColumnStats.getFirstValue(measure)));
                        dataPoints.add(new UnivariateDataPoint(pixelColumnStats.getMinTimestamp(measure), pixelColumnStats.getMinValue(measure)));
                        dataPoints.add(new UnivariateDataPoint(pixelColumnStats.getMaxTimestamp(measure), pixelColumnStats.getMaxValue(measure)));
                        dataPoints.add(new UnivariateDataPoint(pixelColumnStats.getLastTimestamp(measure), pixelColumnStats.getLastValue(measure)));
                    }
                }
                // compute statistics
                count += 1;
                if(max < pixelColumnStats.getMaxValue(measure)) max = pixelColumnStats.getMaxValue(measure);
                if(min > pixelColumnStats.getMinValue(measure)) min = pixelColumnStats.getMinValue(measure);
                sum += pixelColumnStats.getMaxValue(measure) + pixelColumnStats.getMinValue(measure);
            }
            DoubleSummaryStatistics measureStats = new
                    DoubleSummaryStatistics(count, min, max, sum);
            measureStatsMap.put(measure, measureStats);
            resultData.put(measure, dataPoints);
        }
        queryTime = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
        stopwatch.stop();

        // Prefetching
        if(gotData) prefetchManager.prefetch(from, to, measures, pixelColumnInterval, query, aggFactor);

        resultData.forEach((k, v) -> v.sort(Comparator.comparingLong(UnivariateDataPoint::getTimestamp)));
        queryResults.setData(resultData);
        queryResults.setMeasureStats(measureStatsMap);
        queryResults.setError(finalError);
        queryResults.setFlag(errorCalculator.hasError());
        queryResults.setQueryTime(queryTime);
        queryResults.setTimeRange(new TimeRange(from, to));
        queryResults.setAggFactor(aggFactor);
        return queryResults;

    }



}
