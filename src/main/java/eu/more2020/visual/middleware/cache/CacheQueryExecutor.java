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
        boolean gotData = false;
        QueryResults queryResults = new QueryResults();
        ViewPort viewPort = query.getViewPort();

        long pixelColumnInterval = (to - from) / viewPort.getWidth();
        double queryTime = 0;

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

        LOG.debug("Overlapping intervals {}", overlappingSpans.stream().map(span -> span.getAggregateInterval() + " (" + span.getMeasures() + ")").collect(Collectors.joining(", ")));

        dataProcessor.processDatapoints(from, to, query, viewPort, pixelColumns, overlappingSpans);
        errorCalculator.calculateValidColumnsErrors(query, pixelColumns, viewPort, pixelColumnInterval);
        List<MultivariateTimeInterval> missingMultiIntervals = errorCalculator.getMissingIntervals();
        List<TimeInterval> missingIntervals = missingMultiIntervals.stream().map(MultivariateTimeInterval::getInterval).collect(Collectors.toList());
        List<List<Integer>> missingMeasures = missingMultiIntervals.stream().map(MultivariateTimeInterval::getMeasures).collect(Collectors.toList());

        double coveragePercentages = 0.0;
        double aggFactors = 0.0;
        for (TimeSeriesSpan overlappingSpan : overlappingSpans) {
            long size = overlappingSpan.getAggregateInterval(); // ms
            if(size == -1) continue; // if raw data continue
            long spanPixelColumnInterval = (to - from) / overlappingSpan.getWidth();
            double coveragePercentage = overlappingSpan.percentage(query); // coverage
            int spanAggFactor = (int) ((double) (spanPixelColumnInterval) / size);
            aggFactors += coveragePercentage * spanAggFactor;
            coveragePercentages += coveragePercentage;
        }
        aggFactor = coveragePercentages != 0 ? (int) Math.ceil(aggFactors / coveragePercentages) : aggFactor;
        if(errorCalculator.hasError()){
            updateAggFactor();
            List<Integer> measuresWithError = errorCalculator.getMeasuresWithError();
            // Initialize ranges and measures to get all errored data.
            missingIntervals = new ArrayList<>();
            missingIntervals.add(new TimeRange(from, to));
            missingMeasures = new ArrayList<>();
            missingMeasures.add(measuresWithError);
            LOG.debug("Getting {} for measures {}", missingIntervals, missingMeasures);
        }
        LOG.debug("Agg factor = {}", aggFactor);
        if(missingIntervals.size() > 0) gotData = true;
        // Fetch the missing data from the data source.
        List<TimeSeriesSpan> missingTimeSeriesSpans =
                dataProcessor.getMissingAndAddToPixelColumns(from, to, missingMeasures, viewPort, missingIntervals, query, queryResults, aggFactor, pixelColumns);
        // Add them all to the cache.
        cacheManager.addToCache(missingTimeSeriesSpans);

        // Recalculate error
        Map<Integer, Double> finalError  = errorCalculator.calculateTotalError(query, pixelColumns, viewPort, pixelColumnInterval);
        if(errorCalculator.hasError()) {
            gotData = true;
            List<Integer> measuresWithError = errorCalculator.getMeasuresWithError();
            // Initialize ranges and measures to get all errored data.
            missingIntervals = new ArrayList<>();
            missingIntervals.add(new TimeRange(from, to));
            missingMeasures = new ArrayList<>();
            missingMeasures.add(measuresWithError);
            LOG.info("Cached data are above error bound. Fetching {}: for {} ", missingIntervals, measuresWithError);
            query.setQueryMethod(QueryMethod.M4);
            long timeStart = System.currentTimeMillis();
            dataProcessor.getMissingAndAddToPixelColumns(from, to, missingMeasures, viewPort, missingIntervals, query, queryResults, 1, pixelColumns);
            queryResults.setProgressiveQueryTime((System.currentTimeMillis() - timeStart) / 1000F);
            errorCalculator.getMeasuresWithError().forEach(m -> finalError.put(m, 0.0)); // set max error to 0
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
                    if (filterMin < pixelColumnStats.getMinValue(measure) &&
                            filterMax > pixelColumnStats.getMaxValue(measure)) {
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
