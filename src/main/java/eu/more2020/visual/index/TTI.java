package eu.more2020.visual.index;

import com.google.common.base.Stopwatch;
import eu.more2020.visual.datasource.DataSource;
import eu.more2020.visual.datasource.DataSourceFactory;
import eu.more2020.visual.domain.*;
import eu.more2020.visual.domain.Dataset.AbstractDataset;
import eu.more2020.visual.domain.Query.Query;
import eu.more2020.visual.domain.Query.QueryMethod;
import eu.more2020.visual.util.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Time;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class TTI {

    private static final Logger LOG = LoggerFactory.getLogger(TTI.class);

    private final AbstractDataset dataset;

    private final DataSource dataSource;

    private int aggFactor;

    private int DATA_REDUCTION_RATIO = 10;

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
        aggFactor = 5;
    }

    public QueryResults executeQueryM4(Query query) {
        long from = query.getFrom();
        long to = query.getTo();
        QueryResults queryResults = new QueryResults();
        ViewPort viewPort = query.getViewPort();
        double queryTime = 0;
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
        //LOG.debug("Created {} pixel columns: {}", viewPort.getWidth(), pixelColumns.stream().map(PixelColumn::getIntervalString).collect(Collectors.joining(", ")));
        // Query the interval tree for all spans that overlap the query interval.
        List<TimeSeriesSpan> overlappingSpans = StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(intervalTree.overlappers(query), 0), false)
                // Filter out spans with aggregate interval larger than the pixel column interval.
                // This way, each of the groups of the resulting spans will overlap at most two pixel columns.
                .filter(span -> span.getAggregateInterval() <= pixelColumnInterval)
                .collect(Collectors.toList());
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
        List<TimeInterval> missingIntervals = maxErrorEvaluator.getMissingRanges();
        missingIntervals = DateTimeUtil.correctIntervals(from, to, viewPort.getWidth(), missingIntervals);
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
        if(hasError) {
            pixelColumns = new ArrayList<>();
            for (long i = 0; i < viewPort.getWidth(); i++) {
                long pixelFrom = from + (i * pixelColumnInterval);
                long pixelTo = pixelFrom + pixelColumnInterval;
                PixelColumn pixelColumn = new PixelColumn(pixelFrom, pixelTo, measures, viewPort);
                pixelColumns.add(pixelColumn);
            }
            missingIntervals = new ArrayList<>();
            missingIntervals.add(new TimeRange(from, to));
            missingIntervals = DateTimeUtil.correctIntervals(from, to, viewPort.getWidth(), missingIntervals);
            Query query1 = new Query(query.getFrom(), query.getTo(), query.getAccuracy(), QueryMethod.M4_MULTI,
                    query.getMeasures(), query.getViewPort(), query.getOpType());
            LOG.info("Cached data are below error bound. Fetching {}: ", missingIntervals);
            getMissing(from, to, measures, viewPort, missingIntervals, pixelColumns, query1, queryResults, 1);
        }
        else {
            // Fetch the missing data from the data source.
            LOG.info("Unable to Determine Errors: " + missingIntervals);
            getMissing(from, to, measures, viewPort, missingIntervals, pixelColumns, query, queryResults, aggFactor);
        }

        // Recalculate error
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
        for (int m : measures) {
            double measureError = error.get(m) / (viewPort.getHeight() * viewPort.getWidth());
            error.put(m, measureError);
        }

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
        resultData.forEach((k, v) -> v.sort(Comparator.comparingLong(UnivariateDataPoint::getTimestamp)));
        queryResults.setData(resultData);
        queryResults.setError(error);
        queryResults.setFlag(hasError);
        queryResults.setQueryTime(queryTime);
        queryResults.setAggFactor(aggFactor);
        return queryResults;
    }


    public QueryResults executeQueryMinMax(Query query) {
        long from = query.getFrom();
        long to = query.getTo();
        QueryResults queryResults = new QueryResults();
        ViewPort viewPort = query.getViewPort();
        double queryTime = 0;
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
        //LOG.debug("Created {} pixel columns: {}", viewPort.getWidth(), pixelColumns.stream().map(PixelColumn::getIntervalString).collect(Collectors.joining(", ")));
        // Query the interval tree for all spans that overlap the query interval.
        List<TimeSeriesSpan> overlappingSpans = StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(intervalTree.overlappers(query), 0), false)
                // Filter out spans with aggregate interval larger than the pixel column interval.
                // This way, each of the groups of the resulting spans will overlap at most two pixel columns.
                .filter(span -> span.getAggregateInterval() <= pixelColumnInterval)
                .collect(Collectors.toList());
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
        List<TimeInterval> missingIntervals = maxErrorEvaluator.getMissingRanges();
        missingIntervals = DateTimeUtil.correctIntervals(from, to, viewPort.getWidth(), missingIntervals);

        // Find the part of the query interval that is not covered by the spans in the interval tree.
        LOG.info("Unable to Determine Errors: " + missingIntervals);
        // Fetch the missing data from the data source.
        getMissing(from, to, measures, viewPort, missingIntervals, pixelColumns, query, queryResults, aggFactor);

        // Calculate error
        maxErrorEvaluator = new MaxErrorEvaluator(measures, viewPort, pixelColumns);
        pixelColumnErrors = maxErrorEvaluator.computeMaxPixelErrorsPerColumnAndMeasure();
        Map<Integer, Double> error = new HashMap<>();
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
        boolean hasError = true;
        for (int m : measures) {
            double measureError = error.get(m) / (viewPort.getHeight() * viewPort.getWidth());
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
            missingIntervals = new ArrayList<>();
            missingIntervals.add(new TimeRange(from, to));
            missingIntervals = DateTimeUtil.correctIntervals(from, to, viewPort.getWidth(), missingIntervals);
//            aggFactor = 1 + aggFactor;
            LOG.info("Fetching missing data from data source");
            Query query1 = new Query(query.getFrom(), query.getTo(), query.getAccuracy(), QueryMethod.M4_MULTI,
                    query.getMeasures(), query.getViewPort(), query.getOpType());
            getMissing(from, to, measures, viewPort, missingIntervals, pixelColumns, query1, queryResults, 1);
        }

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
        resultData.forEach((k, v) -> v.sort(Comparator.comparingLong(UnivariateDataPoint::getTimestamp)));
        queryResults.setData(resultData);
        queryResults.setError(error);
        queryResults.setFlag(hasError);
        queryResults.setQueryTime(queryTime);
        queryResults.setAggFactor(aggFactor);
        return queryResults;
    }

    private void getMissing(long from, long to, List<Integer> measures, ViewPort viewPort,
                            List<TimeInterval> missingIntervals, List<PixelColumn> pixelColumns,
                            Query query, QueryResults queryResults, int aggFactor) {

        if (missingIntervals.size() >= 1) {
            // Create time series spans from the missing data and insert them into the interval tree.
            long aggregateInterval = (to - from) / ((long) aggFactor * viewPort.getWidth());
            List<TimeSeriesSpan> timeSeriesSpans = null;
            if(aggregateInterval < dataset.getSamplingInterval().toMillis() * DATA_REDUCTION_RATIO){ // get raw data
                List<DataPoint> missingDataPointList = null;
                DataPoints missingDataPoints = null;
                LOG.info("Fetching missing data from data source");
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
        long size = 0L;
        for (TimeSeriesSpan span : intervalTree) {
            size += span.calculateDeepMemorySize();
        }
        return size;
    }

}
