package eu.more2020.visual.middleware.cache;

import com.google.common.base.Stopwatch;
import eu.more2020.visual.middleware.datasource.DataSource;
import eu.more2020.visual.middleware.datasource.DataSourceFactory;
import eu.more2020.visual.middleware.domain.Dataset.AbstractDataset;
import eu.more2020.visual.middleware.domain.Query.Query;
import eu.more2020.visual.middleware.domain.Query.QueryMethod;
import eu.more2020.visual.middleware.domain.*;
import eu.more2020.visual.middleware.util.DateTimeUtil;
import org.ehcache.sizeof.SizeOf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class MinMaxCache {

    private static final Logger LOG = LoggerFactory.getLogger(MinMaxCache.class);

    private final AbstractDataset dataset;

    private final DataSource dataSource;

    private int aggFactor;

    private int dataReductionRatio = 4;

    private final double prefetchingFactor;
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
    public MinMaxCache(AbstractDataset dataset, double prefetchingFactor, int aggFactor, int dataReductionRatio) {
        this.dataset = dataset;
        this.dataSource = DataSourceFactory.getDataSource(dataset);
        this.prefetchingFactor = prefetchingFactor;
        intervalTree = new IntervalTree<>();
        this.dataReductionRatio = dataReductionRatio;
        this.aggFactor = aggFactor;
    }

    void updateAggFactor(){
        aggFactor *= 2;
    }

//    public QueryResults executeQueryM4(Query query) {
//        long from = query.getFrom();
//        long to = query.getTo();
//        QueryResults queryResults = new QueryResults();
//        ViewPort viewPort = query.getViewPort();
//        long pixelColumnInterval = (to - from) / viewPort.getWidth();
//
//        LOG.info("Normal width : {}", viewPort.getWidth());
//        double queryTime = 0;
//        LOG.debug("Pixel column interval: " + pixelColumnInterval + " ms");
//        List<Integer> measures = Optional.ofNullable(query.getMeasures()).orElse(dataset.getMeasures());
//
//        List<PixelColumn> pixelColumns = new ArrayList<>();
//        for (long i = 0; i < viewPort.getWidth(); i++) {
//            long pixelFrom = from + (i * pixelColumnInterval);
//            long pixelTo = pixelFrom + pixelColumnInterval;
//            PixelColumn pixelColumn = new PixelColumn(pixelFrom, pixelTo, measures, viewPort);
//            pixelColumns.add(pixelColumn);
//        }
////        LOG.debug("Created {} pixel columns: {}", viewPort.getWidth(), pixelColumns.stream().map(PixelColumn::getIntervalString).collect(Collectors.joining(", ")));
//        // Query the interval tree for all spans that overlap the query interval.
//
//        List<TimeSeriesSpan> overlappingSpans = StreamSupport.stream(
//                        Spliterators.spliteratorUnknownSize(intervalTree.overlappers(query), 0), false)
//                // Filter out spans with aggregate interval larger than the pixel column interval.
//                // This way, each of the groups of the resulting spans will overlap at most two pixel columns.
//                .filter(span -> pixelColumnInterval >= 2 * span.getAggregateInterval())
//                .collect(Collectors.toList());
//
//        for (TimeSeriesSpan span : overlappingSpans) {
//            if (span instanceof AggregateTimeSeriesSpan) {
//                Iterator<AggregatedDataPoint> iterator = ((AggregateTimeSeriesSpan) span).iterator(from, to);
//                while (iterator.hasNext()) {
//                    AggregatedDataPoint aggregatedDataPoint = iterator.next();
//                    addAggregatedDataPointToPixelColumns(query, pixelColumns, aggregatedDataPoint, viewPort);
//                }
//            }
//            else if (span instanceof RawTimeSeriesSpan){
//                Iterator<DataPoint> iterator = ((RawTimeSeriesSpan) span).iterator(from, to);
//                while (iterator.hasNext()) {
//                    DataPoint dataPoint = iterator.next();
//                    addDataPointToPixelColumns(query, pixelColumns, dataPoint, viewPort);
//                }
//            }
//            else{
//                throw new IllegalArgumentException("Time Series Span Read Error");
//            }
//        }
//        LOG.debug("Overlapping intervals {}", overlappingSpans.stream().map(span -> "" + span.getAggregateInterval() + " (" +query.percentage(span) + ")").collect(Collectors.joining(", ")));
//        MaxErrorEvaluator maxErrorEvaluator = new MaxErrorEvaluator(measures, viewPort, pixelColumns);
//        List<List<Integer>> pixelColumnErrors = maxErrorEvaluator.computeMaxPixelErrorsPerColumnAndMeasure();
//        List<TimeInterval> missingIntervals = maxErrorEvaluator.getMissingRanges();
//        missingIntervals = DateTimeUtil.groupIntervals(pixelColumnInterval, missingIntervals);
//        LOG.info("Unable to Determine Errors: " + missingIntervals);
//
//        // Find the part of the query interval that is not covered by the spans in the interval tree.
//        Map<Integer, Double> error = new HashMap<>();
//        for (int m : measures) error.put(m, 0.0);
//        int validColumns = 0;
//        for (List<Integer> pixelColumnError : pixelColumnErrors) {
//            if(pixelColumnError == null) continue;
//            int i = 0;
//            validColumns ++;
//            for (int m : measures) {
//                final Double data = error.get(m);
//                final int val =  pixelColumnError.get(i);
//                error.put(m, data + val);
//                i++;
//            }
//        }
//        boolean hasError = true;
//        LOG.debug("Valid columns: {}", validColumns);
//        for (int m : measures) {
//            double measureError = error.get(m) / (viewPort.getHeight() * validColumns);
//            LOG.info("Measure has error: {}", measureError);
//            hasError = measureError > 1 - query.getAccuracy();
//            error.put(m, measureError);
//        }
//        if(hasError) {
//            pixelColumns = new ArrayList<>();
//            for (long i = 0; i < viewPort.getWidth(); i++) {
//                long pixelFrom = from + (i * pixelColumnInterval);
//                long pixelTo = pixelFrom + pixelColumnInterval;
//                PixelColumn pixelColumn = new PixelColumn(pixelFrom, pixelTo, measures, viewPort);
//                pixelColumns.add(pixelColumn);
//            }
//            updateAggFactor();
//            missingIntervals = new ArrayList<>();
//            missingIntervals.add(new TimeRange(from, to));
//            LOG.info("Cached data are above error bound. Fetching {}: ", missingIntervals);
//            query.setQueryMethod(QueryMethod.M4_MULTI);
//            getMissing(from, to, measures, viewPort, missingIntervals, pixelColumns, query, queryResults, aggFactor);
//            measures.forEach(m -> error.put(m, 0.0)); // set max error to 0
//        }
//        else {
//            // Fetch the missing data from the data source.
////          LOG.info("Unable to Determine Errors: " + missingIntervals);
//            getMissing(from, to, measures, viewPort, missingIntervals, pixelColumns, query, queryResults, aggFactor);
//            // Recalculate error
//            maxErrorEvaluator = new MaxErrorEvaluator(measures, viewPort, pixelColumns);
//            pixelColumnErrors = maxErrorEvaluator.computeMaxPixelErrorsPerColumnAndMeasure();
//            for (int m : measures) error.put(m, 0.0);
//            for (List<Integer> pixelColumnError : pixelColumnErrors) {
//                if(pixelColumnError == null) continue;
//                int i = 0;
//                for (int m : measures) {
//                    final Double data = error.get(m);
//                    final int val = pixelColumnError.get(i);
//                    error.put(m, data + val);
//                    i++;
//                }
//            }
//            for (int m : measures) {
//                double measureError = error.get(m) / (viewPort.getHeight() * viewPort.getWidth());
//                error.put(m, measureError);
//            }
//        }
//
//        Map<Integer, List<UnivariateDataPoint>> resultData = new HashMap<>();
//        for (int measure : measures) {
//            List<UnivariateDataPoint> dataPoints = new ArrayList<>();
//            for (PixelColumn pixelColumn : pixelColumns) {
//                Stats pixelColumnStats = pixelColumn.getStats();
//                if (pixelColumnStats.getCount() == 0) {
//                    continue;
//                }
//                // filter
//                if(true) {
//                    dataPoints.add(new UnivariateDataPoint(pixelColumnStats.getFirstTimestamp(measure), pixelColumnStats.getFirstValue(measure)));
//                    dataPoints.add(new UnivariateDataPoint(pixelColumnStats.getMinTimestamp(measure), pixelColumnStats.getMinValue(measure)));
//                    dataPoints.add(new UnivariateDataPoint(pixelColumnStats.getMaxTimestamp(measure), pixelColumnStats.getMaxValue(measure)));
//                    dataPoints.add(new UnivariateDataPoint(pixelColumnStats.getLastTimestamp(measure), pixelColumnStats.getLastValue(measure)));
//                }
//            }
//            resultData.put(measure, dataPoints);
//        }
//        resultData.forEach((k, v) -> v.sort(Comparator.comparingLong(UnivariateDataPoint::getTimestamp)));
//        queryResults.setData(resultData);
//        queryResults.setError(error);
//        queryResults.setFlag(hasError);
//        queryResults.setQueryTime(queryTime);
//        queryResults.setAggFactor(aggFactor);
//        return queryResults;
//    }

    public boolean areListsEqual(List<Integer> list1, List<Integer> list2){
        Collections.sort(list1);
        Collections.sort(list2);
        return list1.equals(list2);
    }

    public QueryResults executeQueryMinMax(Query query) {
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
        //LOG.debug("Created {} pixel columns: {}", viewPort.getWidth(), pixelColumns.stream().map(PixelColumn::getIntervalString).collect(Collectors.joining(", ")));
        // Query the interval tree for all spans that overlap the query interval.
        List<TimeSeriesSpan> overlappingSpans = StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(intervalTree.overlappers(query), 0), false)
                // Keep only spans with an aggregate interval that is half or less than the pixel column interval to ensure at least one fully contained in every pixel column that the span fully overlaps
                // This way, each of the groups of the resulting spans will overlap at most two pixel columns.
                .filter(span -> pixelColumnInterval >= 2 * span.getAggregateInterval()
                && areListsEqual(span.getMeasures(), query.getMeasures()))
                .collect(Collectors.toList());
        LOG.debug("Overlapping intervals {}", overlappingSpans.stream().map(span -> span.getAggregateInterval() + " (" + span.percentage(query) + ")").collect(Collectors.joining(", ")));

        for (TimeSeriesSpan span : overlappingSpans) {
            if (span instanceof AggregateTimeSeriesSpan) {
                Iterator<AggregatedDataPoint> iterator = ((AggregateTimeSeriesSpan) span).iterator(from, to);
                while (iterator.hasNext()) {
                    AggregatedDataPoint aggregatedDataPoint = iterator.next();
                    addAggregatedDataPointToPixelColumns(query, pixelColumns, aggregatedDataPoint, viewPort);
                }
            }
            else if (span instanceof RawTimeSeriesSpan){
                Iterator<DataPoint> iterator = ((RawTimeSeriesSpan) span).iterator(from, to);
                while (iterator.hasNext()) {
                    DataPoint dataPoint = iterator.next();
                    addDataPointToPixelColumns(query, pixelColumns, dataPoint, viewPort);
                }
            }
            else{
                throw new IllegalArgumentException("Time Series Span Read Error");
            }
        }
        MaxErrorEvaluator maxErrorEvaluator = new MaxErrorEvaluator(measures, viewPort, pixelColumns);
        List<List<Integer>> pixelColumnErrors = maxErrorEvaluator.computeMaxPixelErrorsPerColumnAndMeasure();
        // Find the part of the query interval that is not covered by the spans in the interval tree.
        Map<Integer, Double> error = new HashMap<>();
        for (int m : measures) error.put(m, 0.0);
        int validColumns = 0;
        for (List<Integer> pixelColumnError : pixelColumnErrors) {
            if(pixelColumnError == null) continue;
            int i = 0;
            validColumns ++;
            for (int m : measures) {
                final Double data = error.get(m);
                final int val =  pixelColumnError.get(i);
                error.put(m, data + val);
                i++;
            }
        }
        boolean hasError = true;
        LOG.debug("Valid columns: {}", validColumns);
        for (int m : measures) {
            double measureError = error.get(m) / (viewPort.getHeight() * validColumns);
            LOG.info("Measure has error: {}", measureError);
            hasError = measureError > 1 - query.getAccuracy();
            error.put(m, measureError);
        }
        List<TimeInterval> missingIntervals = maxErrorEvaluator.getMissingRanges();
        missingIntervals = DateTimeUtil.groupIntervals(pixelColumnInterval, missingIntervals);
        if(!missingIntervals.isEmpty()) gotData = true;
        LOG.info("Unable to Determine Errors: " + missingIntervals);
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
        aggFactor = Math.min(aggFactor, 10);
        if(hasError){
            updateAggFactor();
        }
        LOG.debug("Agg factor = {}", aggFactor);
        // Fetch the missing data from the data source.
        getMissing(from, to, measures, viewPort, missingIntervals, pixelColumns, query, queryResults, aggFactor);

        // Calculate error
        maxErrorEvaluator = new MaxErrorEvaluator(measures, viewPort, pixelColumns);
        pixelColumnErrors = maxErrorEvaluator.computeMaxPixelErrorsPerColumnAndMeasure();
        error = new HashMap<>();
        for (int m : measures) error.put(m, 0.0);
        for (List<Integer> pixelColumnError : pixelColumnErrors) {
            if(pixelColumnError == null) continue;
            int i = 0;
            for (int m : measures) {
                final Double data = error.get(m);
                final int val = pixelColumnError.get(i);
                error.put(m, data + val);
                i++;
            }
        }
        hasError = true;
        for (int m : measures) {
            double measureError = error.get(m) / (viewPort.getHeight() * viewPort.getWidth());
            LOG.info("Measure has error: {}", measureError);
            hasError = measureError > 1 - query.getAccuracy();
            error.put(m, measureError);
        }
        if(hasError) {
            pixelColumns = new ArrayList<>();
            for (long i = 0; i < viewPort.getWidth(); i++) {
                long pixelFrom = from + (i * pixelColumnInterval);
                long pixelTo = pixelFrom + pixelColumnInterval;
                PixelColumn pixelColumn = new PixelColumn(pixelFrom, pixelTo, measures, viewPort);
                pixelColumns.add(pixelColumn);
            }
            gotData = true;
            missingIntervals = new ArrayList<>();
            missingIntervals.add(new TimeRange(from, to));
            LOG.info("Cached data are above error bound. Fetching {}: ", missingIntervals);
            LOG.info("Fetching missing data from data source");
            query.setQueryMethod(QueryMethod.M4_MULTI);
            long timeStart = System.currentTimeMillis();
            getMissing(from, to, measures, viewPort, missingIntervals, pixelColumns, query, queryResults, 1);
            queryResults.setProgressiveQueryTime((System.currentTimeMillis() - timeStart) / 1000F);
            Map<Integer, Double> finalError = error;
            measures.forEach(m -> finalError.put(m, 0.0)); // set max error to 0
        }
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
                if (pixelColumnStats.getCount() <= 0) {
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
            DoubleSummaryStatistics measureStats = new DoubleSummaryStatistics(count, min, max, sum);
            measureStatsMap.put(measure, measureStats);
            resultData.put(measure, dataPoints);
        }
        queryTime = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
        stopwatch.stop();
        if(prefetchingFactor != 0 && gotData)
            prefetch(from, to, measures, pixelColumnInterval, query);
        resultData.forEach((k, v) -> v.sort(Comparator.comparingLong(UnivariateDataPoint::getTimestamp)));
        queryResults.setData(resultData);
        queryResults.setMeasureStats(measureStatsMap);
        queryResults.setError(error);
        queryResults.setFlag(hasError);
        queryResults.setQueryTime(queryTime);
        queryResults.setTimeRange(new TimeRange(from, to));
        queryResults.setAggFactor(aggFactor);
        return queryResults;
    }

    long[] extendInterval(long from, long to, int width, double factor){
        long pixelColumnInterval = (to - from) / width;
        int noOfColumns = (int) ((width * factor) / 2);
        long newFrom = Math.max(dataset.getTimeRange().getFrom(), from - (noOfColumns * pixelColumnInterval));
        long newTo = Math.min(dataset.getTimeRange().getTo(), (noOfColumns * pixelColumnInterval) + to);
        return new long[]{newFrom, newTo};
    }

    private List<TimeInterval> prefetch(long from, long to, List<Integer> measures,
                                        long pixelColumnInterval, Query query){
        List<TimeInterval> prefetchingIntervals = new ArrayList<>();
        ViewPort viewPort = query.getViewPort();
        // For the prefetching we add pixel columns to the left and right depending to the prefetching factor.
        // We create a new viewport based on the new width that results from prefetching. The new viewport has more columns but the same interval.
        long[] prefetchingInterval = extendInterval(from, to, query.getViewPort().getWidth(), prefetchingFactor);
        long prefetchingFrom = prefetchingInterval[0];
        long prefetchingTo = prefetchingInterval[1];
        int prefetchingWidth = (int) (viewPort.getWidth() + (prefetchingTo - to) / pixelColumnInterval +  (from - prefetchingFrom) / pixelColumnInterval);
        Query prefetchingQuery = new Query(prefetchingFrom, prefetchingTo, query.getAccuracy(), query.getFilter(), query.getQueryMethod(), query.getMeasures(),
                new ViewPort(prefetchingWidth, query.getViewPort().getHeight()), query.getOpType());
        prefetchingIntervals.add(new TimeRange(prefetchingFrom, prefetchingTo));
        prefetchingIntervals = DateTimeUtil.groupIntervals(pixelColumnInterval, prefetchingIntervals);
        LOG.info("Prefetching: {}", prefetchingIntervals.stream().map(p -> p.getIntervalString()).collect(Collectors.joining(", ")));
        if (prefetchingIntervals.size() >= 1) {
            // Create time series spans from the missing data and insert them into the interval tree.
            long aggregateInterval = (to - from) / ((long) aggFactor * viewPort.getWidth());
            long numberOfAggDataPoints = 4L * aggFactor * viewPort.getWidth();
            long numberOfRawDataPoints = prefetchingIntervals.stream().mapToLong(m -> (m.getTo() - m.getFrom()) / dataset.getSamplingInterval().toMillis()).sum();

            List<TimeSeriesSpan> timeSeriesSpans = null;

            if(numberOfAggDataPoints > (numberOfRawDataPoints / dataReductionRatio)){ // get raw data
                List<DataPoint> missingDataPointList = null;
                DataPoints missingDataPoints = null;
                LOG.info("Prefetching {} missing raw data from data source", numberOfRawDataPoints);
                missingDataPoints = dataSource.getDataPoints(from, to, prefetchingIntervals, measures);
                missingDataPointList = StreamSupport.stream(missingDataPoints.spliterator(), false).collect(Collectors.toList());
                LOG.info("Fetched prefetching data from data source");
                timeSeriesSpans = TimeSeriesSpanFactory.createRaw(missingDataPointList, measures, prefetchingIntervals);
            }
            else{
                List<AggregatedDataPoint> missingDataPointList = null;
                AggregatedDataPoints missingDataPoints = null;
                LOG.info("Fetching missing data from data source");
                missingDataPoints = dataSource.getAggregatedDataPoints(from, to, prefetchingIntervals, query.getQueryMethod(), measures,
                        aggFactor * viewPort.getWidth());
                missingDataPointList = StreamSupport.stream(missingDataPoints.spliterator(), false).collect(Collectors.toList());
                LOG.info("Fetched missing data from data source");

                // Add the data points fetched from the data store to the pixel columns
                timeSeriesSpans = TimeSeriesSpanFactory.createAggregate(missingDataPointList, measures, prefetchingIntervals, aggregateInterval);
            }
            intervalTree.insertAll(timeSeriesSpans);
            LOG.info("Inserted new time series spans into interval tree");
        }
        return prefetchingIntervals;
    }

    private void getMissing(long from, long to, List<Integer> measures, ViewPort viewPort, List<TimeInterval> missingIntervals, List<PixelColumn> pixelColumns,
                            Query query, QueryResults queryResults, int aggFactor) {

        if (missingIntervals.size() >= 1) {
            // Create time series spans from the missing data and insert them into the interval tree.
            long numberOfAggDataPoints = 4L * viewPort.getWidth();
            long numberOfRawDataPoints = missingIntervals.stream().mapToLong(m -> (m.getTo() - m.getFrom()) / dataset.getSamplingInterval().toMillis()).sum();

            List<TimeSeriesSpan> timeSeriesSpans = null;

            if(numberOfAggDataPoints > (numberOfRawDataPoints / dataReductionRatio)){ // get raw data
                List<DataPoint> missingDataPointList = null;
                DataPoints missingDataPoints = null;
                LOG.info("Fetching {} missing raw data from data source", numberOfRawDataPoints);
                missingDataPoints = dataSource.getDataPoints(from, to, missingIntervals, measures);
                missingDataPointList = StreamSupport.stream(missingDataPoints.spliterator(), false).collect(Collectors.toList());
                LOG.info("Fetched missing data from data source");

                // Add the data points fetched from the data store to the pixel columns
                missingDataPointList.forEach(dataPoint -> {
                        addDataPointToPixelColumns(query, pixelColumns, dataPoint, viewPort);
                });
                LOG.debug("Added fetched data points to pixel columns");
                timeSeriesSpans = TimeSeriesSpanFactory.createRaw(missingDataPointList, measures, missingIntervals);
            }
            else{
                List<AggregatedDataPoint> missingDataPointList = null;
                AggregatedDataPoints missingDataPoints = null;
                LOG.info("Fetching missing data from data source");
                missingDataPoints = dataSource.getAggregatedDataPoints(from, to, missingIntervals, query.getQueryMethod(), measures,
                        aggFactor * viewPort.getWidth());
                missingDataPointList = StreamSupport.stream(missingDataPoints.spliterator(), false).collect(Collectors.toList());
                LOG.info("Fetched missing data from data source");

                // Add the data points fetched from the data store to the pixel columns
                missingDataPointList.forEach(aggregatedDataPoint -> {
                        addAggregatedDataPointToPixelColumns(query, pixelColumns, aggregatedDataPoint, viewPort);
                });
                LOG.debug("Added fetched data points to pixel columns");
                long aggregateInterval = (to - from) / ((long) aggFactor * viewPort.getWidth());
                timeSeriesSpans = TimeSeriesSpanFactory.createAggregate(missingDataPointList, measures, missingIntervals, aggregateInterval);
            }
            timeSeriesSpans.forEach(t -> queryResults.setIoCount(queryResults.getIoCount() + Arrays.stream(t.getCounts()).sum()));
            intervalTree.insertAll(timeSeriesSpans);
            LOG.info("Inserted new time series spans into interval tree");
        }
    }

    private int getPixelColumnForTimestamp(long timestamp, long from, long to, int width) {
        long aggregateInterval = (to - from) / width;
        return (int) ((timestamp - from) / aggregateInterval);
    }

    private void addAggregatedDataPointToPixelColumns(Query query, List<PixelColumn> pixelColumns, AggregatedDataPoint aggregatedDataPoint, ViewPort viewPort) {
        if(!query.encloses(aggregatedDataPoint)) return;
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

    private void addDataPointToPixelColumns(Query query, List<PixelColumn> pixelColumns, DataPoint dataPoint, ViewPort viewPort){
        if(!query.contains(dataPoint.getTimestamp())) return;
        int pixelColumnIndex = getPixelColumnForTimestamp(dataPoint.getTimestamp(), query.getFrom(), query.getTo(), viewPort.getWidth());
        if (pixelColumnIndex < viewPort.getWidth()) {
            pixelColumns.get(pixelColumnIndex).addDataPoint(dataPoint);
        }
    }


    /**
     * Calculates the deep memory size of this instance.
     *
     * @return The deep memory size in bytes.
     */
    public long calculateDeepMemorySize() {
        SizeOf sizeOf = SizeOf.newInstance();
        return sizeOf.deepSizeOf(intervalTree);
    }

}
