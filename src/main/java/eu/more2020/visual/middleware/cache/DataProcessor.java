package eu.more2020.visual.middleware.cache;

import eu.more2020.visual.middleware.datasource.DataSource;
import eu.more2020.visual.middleware.datasource.DataSourceFactory;
import eu.more2020.visual.middleware.datasource.QueryExecutor.QueryExecutor;
import eu.more2020.visual.middleware.domain.*;
import eu.more2020.visual.middleware.domain.Dataset.AbstractDataset;
import eu.more2020.visual.middleware.domain.Query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class DataProcessor {

    private final AbstractDataset dataset;
    private final DataSource dataSource;
    private final int dataReductionRatio;

    public DataProcessor(QueryExecutor queryExecutor, AbstractDataset dataset, int dataReductionRatio){
        this.dataset = dataset;
        this.dataSource = DataSourceFactory.getDataSource(queryExecutor, dataset);
        this.dataReductionRatio = dataReductionRatio;
    }

    private static final Logger LOG = LoggerFactory.getLogger(MinMaxCache.class);

    public void processDatapoints(long from, long to, Query query, ViewPort viewPort,
                                   List<PixelColumn> pixelColumns, List<TimeSeriesSpan> timeSeriesSpans) {
        for (TimeSeriesSpan span : timeSeriesSpans) {
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
                    addDataPointToPixelColumns(query, pixelColumns, dataPoint, span.getMeasures(), viewPort);
                }
            }
            else{
                throw new IllegalArgumentException("Time Series Span Read Error");
            }
        }
    }


    public List<TimeSeriesSpan> getMissing(long from, long to, long pixelColumnInterval,
                                           List<List<Integer>> measures, ViewPort viewPort, List<TimeInterval> missingIntervals, Query query, int aggFactor) {
        List<TimeSeriesSpan> timeSeriesSpans = null;
        if (missingIntervals.size() >= 1) {
            // Create time series spans from the missing data and insert them into the interval tree.

            if(pixelColumnInterval < (dataset.getSamplingInterval().toMillis() * dataReductionRatio)){ // get raw data
                List<DataPoint> missingDataPointList = null;
                DataPoints missingDataPoints = null;
                LOG.info("Fetching missing raw data from data source");
                missingDataPoints = dataSource.getDataPoints(from, to, missingIntervals, measures);
                missingDataPointList = StreamSupport.stream(missingDataPoints.spliterator(), false).collect(Collectors.toList());
                LOG.info("Fetched missing data from data source");
                timeSeriesSpans = TimeSeriesSpanFactory.createRaw(missingDataPointList, missingIntervals, measures);
            }
            else{
                List<AggregatedDataPoint> missingDataPointList = null;
                AggregatedDataPoints missingDataPoints = null;
                LOG.info("Fetching missing data from data source");
                missingDataPoints = dataSource.getAggregatedDataPoints(from, to, missingIntervals, measures, query.getQueryMethod(),
                        aggFactor * viewPort.getWidth());
                missingDataPointList = StreamSupport.stream(missingDataPoints.spliterator(), false).collect(Collectors.toList());
                LOG.info("Fetched missing data from data source");
                long aggregateInterval = (to - from) / ((long) aggFactor * viewPort.getWidth());
                timeSeriesSpans = TimeSeriesSpanFactory.createAggregate(missingDataPointList, missingIntervals, measures, aggregateInterval);
            }
        }
        return timeSeriesSpans;
    }

    public List<TimeSeriesSpan> getMissingAndAddToPixelColumns(long from, long to, List<List<Integer>> measures, ViewPort viewPort, List<TimeInterval> missingIntervals,
                           Query query, QueryResults queryResults, int aggFactor, List<PixelColumn> pixelColumns) {
        List<TimeSeriesSpan> timeSeriesSpans = null;
        if (missingIntervals.size() >= 1) {
            // Create time series spans from the missing data and insert them into the interval tree.
            long numberOfAggDataPoints = 4L * viewPort.getWidth();
            long numberOfRawDataPoints = missingIntervals.stream().mapToLong(m -> (m.getTo() - m.getFrom()) / dataset.getSamplingInterval().toMillis()).sum();


            if(numberOfAggDataPoints > (numberOfRawDataPoints / dataReductionRatio)){ // get raw data
                List<DataPoint> missingDataPointList = null;
                DataPoints missingDataPoints = null;
                LOG.info("Fetching {} missing raw data from data source", numberOfRawDataPoints);
                missingDataPoints = dataSource.getDataPoints(from, to, missingIntervals, measures);
                missingDataPointList = StreamSupport.stream(missingDataPoints.spliterator(), false).collect(Collectors.toList());
                LOG.info("Fetched missing data from data source");

                // Add the data points fetched from the data store to the pixel columns
                missingDataPointList.forEach(dataPoint -> {
                    addDataPointToPixelColumns(query, pixelColumns, dataPoint, getMeasuresForTimestamp(missingIntervals, measures, dataPoint.getTimestamp()), viewPort);
                });
                LOG.debug("Added fetched data points to pixel columns");
                timeSeriesSpans = TimeSeriesSpanFactory.createRaw(missingDataPointList, missingIntervals, measures);
            }
            else {
                List<AggregatedDataPoint> missingDataPointList = null;
                AggregatedDataPoints missingDataPoints = null;
                LOG.info("Fetching missing data from data source");
                missingDataPoints = dataSource.getAggregatedDataPoints(from, to, missingIntervals, measures, query.getQueryMethod(),
                        aggFactor * viewPort.getWidth());
                missingDataPointList = StreamSupport.stream(missingDataPoints.spliterator(), false).collect(Collectors.toList());
                LOG.info("Fetched missing data from data source");

                // Add the data points fetched from the data store to the pixel columns
                missingDataPointList.forEach(aggregatedDataPoint -> {
                    addAggregatedDataPointToPixelColumns(query, pixelColumns, aggregatedDataPoint, viewPort);
                });
                LOG.debug("Added fetched data points to pixel columns");
                long aggregateInterval = (to - from) / ((long) aggFactor * viewPort.getWidth());
                timeSeriesSpans = TimeSeriesSpanFactory.createAggregate(missingDataPointList, missingIntervals, measures, aggregateInterval);
            }
            timeSeriesSpans.forEach(t -> queryResults.setIoCount(queryResults.getIoCount() + Arrays.stream(t.getCounts()).sum()));
        }
        return timeSeriesSpans;
    }

    private int getPixelColumnForTimestamp(long timestamp, long from, long to, int width) {
        long aggregateInterval = (to - from) / width;
        return (int) ((timestamp - from) / aggregateInterval);
    }

    private List<Integer> getMeasuresForTimestamp(List<TimeInterval> ranges, List<List<Integer>> measures, long timestamp){
        int index = 0;
        for(TimeInterval range : ranges){
            if(timestamp < range.getTo()) break;
            else index ++;
        }
        return measures.get(index);
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

    private void addDataPointToPixelColumns(Query query, List<PixelColumn> pixelColumns, DataPoint dataPoint, List<Integer> measures, ViewPort viewPort){
        if(!query.contains(dataPoint.getTimestamp())) return;
        int pixelColumnIndex = getPixelColumnForTimestamp(dataPoint.getTimestamp(), query.getFrom(), query.getTo(), viewPort.getWidth());
        if (pixelColumnIndex < viewPort.getWidth()) {
            pixelColumns.get(pixelColumnIndex).addDataPoint(dataPoint, measures);
        }
    }

    public int getDataReductionRatio() {
        return dataReductionRatio;
    }
}
