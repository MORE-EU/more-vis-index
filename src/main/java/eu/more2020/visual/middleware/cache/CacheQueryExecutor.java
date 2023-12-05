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

public class CacheQueryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(MinMaxCache.class);
    private final AbstractDataset dataset;
    private int[] aggFactors;

    public CacheQueryExecutor(AbstractDataset dataset, int aggFactor) {
        this.dataset = dataset;
        this.aggFactors = new int[dataset.getMeasures().size()];
        Arrays.fill(aggFactors, aggFactor);
    }

    void updateAggFactor(int idx){
        aggFactors[idx] *= 2;
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

        Stopwatch stopwatch = Stopwatch.createUnstarted();
        stopwatch.start();
        LOG.debug("Pixel column interval: " + pixelColumnInterval + " ms");
        List<Integer> measures = Optional.ofNullable(query.getMeasures()).orElse(dataset.getMeasures());

        // Initialize Pixel Column
        List<List<PixelColumn>> pixelColumnsPerMeasure = new ArrayList<>(); // A list of pixel columns one for every measure
        for(int i = 0; i < measures.size(); i ++) {
            List<PixelColumn> pixelColumns = new ArrayList<>();
            for (long j = 0; j < viewPort.getWidth(); j++) {
                long pixelFrom = from + (j * pixelColumnInterval);
                long pixelTo = pixelFrom + pixelColumnInterval;
                PixelColumn pixelColumn = new PixelColumn(pixelFrom, pixelTo, viewPort);
                pixelColumns.add(pixelColumn);
            }
            pixelColumnsPerMeasure.add(pixelColumns);
        }

        List<List<TimeSeriesSpan>> overlappingSpansPerMeasure = cacheManager.getFromCache(query, pixelColumnInterval);
        LOG.debug("Overlapping intervals per measure {}", overlappingSpansPerMeasure);
        List<List<TimeInterval>> missingIntervals = new ArrayList<>();
        for(int i = 0; i < measures.size(); i ++){
            List<PixelColumn> pixelColumns =  pixelColumnsPerMeasure.get(i);
            List<TimeSeriesSpan> overlappingSpans = overlappingSpansPerMeasure.get(i);
            dataProcessor.processDatapoints(from, to, viewPort, pixelColumns, overlappingSpans);
            errorCalculator.calculateTotalError(pixelColumns, viewPort, pixelColumnInterval, query.getAccuracy());
            List<TimeInterval> missingIntervalsForMeasure = errorCalculator.getMissingIntervals();

            double coveragePercentages = 0.0;
            double totalAggFactors = 0.0;
            for (TimeSeriesSpan overlappingSpan : overlappingSpans) {
                long size = overlappingSpan.getAggregateInterval(); // ms
                if(size == -1) continue; // if raw data continue
                double coveragePercentage = overlappingSpan.percentage(query); // coverage
                int spanAggFactor = (int) ((double) (pixelColumnInterval) / size);
                totalAggFactors += coveragePercentage * spanAggFactor;
                coveragePercentages += coveragePercentage;
            }
            aggFactors[i] = coveragePercentages != 0 ? (int) Math.ceil(totalAggFactors / coveragePercentages) : aggFactors[i];
            if(errorCalculator.hasError()){
                updateAggFactor(i);
                // Initialize ranges and measures to get all errored data.
                missingIntervalsForMeasure = new ArrayList<>();
                missingIntervalsForMeasure.add(new TimeRange(from, to));
                LOG.debug("Getting {} for measures {}", missingIntervals);
            }
            LOG.debug("Agg factor = {}", aggFactors[i]);
            missingIntervals.add(missingIntervalsForMeasure);
        }

        // Fetch the missing data from the data source.
        List<TimeSeriesSpan> timeSeriesSpans =
                dataProcessor.getMissing(from, to, missingIntervals, measures, aggFactors, viewPort, query.getQueryMethod());
        // TODO: Add to pixel columns

        // Add them all to the cache.
        cacheManager.addToCache(timeSeriesSpans);

        // Recalculate error per measure
        List<Integer> measuresWithError = new ArrayList<>();
        HashMap<Integer, Double> finalError = new HashMap<>();
        for(int i = 0; i < measures.size(); i ++) {
            int measure = measures.get(i);
            List<PixelColumn> pixelColumns = pixelColumnsPerMeasure.get(i);
            double errorForMeasure = errorCalculator.calculateTotalError(pixelColumns, viewPort, pixelColumnInterval, query.getAccuracy());
            if(errorCalculator.hasError()) measuresWithError.add(measure);
            finalError.put(measure, errorForMeasure);
        }
        // Fetch errored measures with M4
        missingIntervals = List.of(List.of(new TimeRange(from, to)));
        int[] m4AggFactors = new int[measuresWithError.size()];
        Arrays.fill(m4AggFactors, 1);
        LOG.info("Cached data are above error bound. Fetching {}: for {} ", missingIntervals, measuresWithError);
        query.setQueryMethod(QueryMethod.M4);
        long timeStart = System.currentTimeMillis();
        dataProcessor.getMissing(from, to, missingIntervals, measuresWithError, m4AggFactors, viewPort, QueryMethod.M4);
        // Set error to 0 for M4 measures
        for (int measure : measuresWithError) finalError.put(measure, 0.0);
        // TODO: Add to pixel columns

        queryResults.setProgressiveQueryTime((System.currentTimeMillis() - timeStart) / 1000F);
        // Query Results

        Map<Integer, List<UnivariateDataPoint>> resultData = new HashMap<>();
        Map<Integer, DoubleSummaryStatistics> measureStatsMap = new HashMap<>();
        for (int i = 0; i < measures.size(); i ++) {
            int count = 0;
            double max = Double.MIN_VALUE;
            double min = Double.MAX_VALUE;
            double sum = 0;
            int measure = measures.get(i);
            List<PixelColumn> pixelColumns = pixelColumnsPerMeasure.get(i);
            List<UnivariateDataPoint> dataPoints = new ArrayList<>();

            for (PixelColumn pixelColumn : pixelColumns) {
                Stats pixelColumnStats = pixelColumn.getStats();
                if (pixelColumnStats.getCount() <= 0) {
                    continue;
                }
                // filter
                if(query.getFilter() == null || query.getFilter().isEmpty()){
                    dataPoints.add(new ImmutableUnivariateDataPoint(pixelColumnStats.getFirstTimestamp(), pixelColumnStats.getFirstValue()));
                    dataPoints.add(new ImmutableUnivariateDataPoint(pixelColumnStats.getMinTimestamp(), pixelColumnStats.getMinValue()));
                    dataPoints.add(new ImmutableUnivariateDataPoint(pixelColumnStats.getMaxTimestamp(), pixelColumnStats.getMaxValue()));
                    dataPoints.add(new ImmutableUnivariateDataPoint(pixelColumnStats.getLastTimestamp(), pixelColumnStats.getLastValue()));
                }
                else {
                    double filterMin = query.getFilter().get(measure)[0];
                    double filterMax = query.getFilter().get(measure)[1];
                    if (filterMin < pixelColumnStats.getMinValue() &&
                            filterMax > pixelColumnStats.getMaxValue()) {
                        dataPoints.add(new ImmutableUnivariateDataPoint(pixelColumnStats.getFirstTimestamp(), pixelColumnStats.getFirstValue()));
                        dataPoints.add(new ImmutableUnivariateDataPoint(pixelColumnStats.getMinTimestamp(), pixelColumnStats.getMinValue()));
                        dataPoints.add(new ImmutableUnivariateDataPoint(pixelColumnStats.getMaxTimestamp(), pixelColumnStats.getMaxValue()));
                        dataPoints.add(new ImmutableUnivariateDataPoint(pixelColumnStats.getLastTimestamp(), pixelColumnStats.getLastValue()));
                    }
                }
                // compute statistics
                count += 1;
                if(max < pixelColumnStats.getMaxValue()) max = pixelColumnStats.getMaxValue();
                if(min > pixelColumnStats.getMinValue()) min = pixelColumnStats.getMinValue();
                sum += pixelColumnStats.getMaxValue() + pixelColumnStats.getMinValue();
            }
            DoubleSummaryStatistics measureStats = new
                    DoubleSummaryStatistics(count, min, max, sum);
            measureStatsMap.put(measure, measureStats);
            resultData.put(measure, dataPoints);
        }
        queryTime = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
        stopwatch.stop();

        // Prefetching
//        prefetchManager.prefetch(from, to, measures, pixelColumnInterval, query, aggFactors);

        resultData.forEach((k, v) -> v.sort(Comparator.comparingLong(UnivariateDataPoint::getTimestamp)));
        queryResults.setData(resultData);
        queryResults.setMeasureStats(measureStatsMap);
        queryResults.setError(finalError);
        queryResults.setFlag(errorCalculator.hasError());
        queryResults.setQueryTime(queryTime);
        queryResults.setTimeRange(new TimeRange(from, to));
        queryResults.setAggFactors(aggFactors);
        return queryResults;

    }



}
