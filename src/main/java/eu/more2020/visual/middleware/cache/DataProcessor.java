package eu.more2020.visual.middleware.cache;

import eu.more2020.visual.middleware.datasource.DataSource;
import eu.more2020.visual.middleware.datasource.DataSourceFactory;
import eu.more2020.visual.middleware.datasource.QueryExecutor.QueryExecutor;
import eu.more2020.visual.middleware.domain.*;
import eu.more2020.visual.middleware.domain.Dataset.AbstractDataset;
import eu.more2020.visual.middleware.domain.Query.Query;
import eu.more2020.visual.middleware.domain.Query.QueryMethod;
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

    private static final Logger LOG = LoggerFactory.getLogger(DataProcessor.class);

    public void processDatapoints(long from, long to, ViewPort viewPort,
                                   List<PixelColumn> pixelColumns, List<TimeSeriesSpan> timeSeriesSpans) {
        for (TimeSeriesSpan span : timeSeriesSpans) {
            if (span instanceof AggregateTimeSeriesSpan) {
                Iterator<AggregatedDataPoint> iterator = ((AggregateTimeSeriesSpan) span).iterator(from, to);
                while (iterator.hasNext()) {
                    AggregatedDataPoint aggregatedDataPoint = iterator.next();
                    addAggregatedDataPointToPixelColumns(from, to, viewPort, pixelColumns, aggregatedDataPoint);
                }
            }
            else if (span instanceof RawTimeSeriesSpan){
                Iterator<UnivariateDataPoint> iterator = ((RawTimeSeriesSpan) span).iterator(from, to);
                while (iterator.hasNext()) {
                    UnivariateDataPoint dataPoint = iterator.next();
                    addDataPointToPixelColumns(from, to, viewPort, pixelColumns, dataPoint);
                }
            }
            else{
                throw new IllegalArgumentException("Time Series Span Read Error");
            }
        }
    }

    public List<TimeSeriesSpan> getMissing(long from, long to, List<List<TimeInterval>> missingIntervals, List<Integer> measures,
                                           int[] aggFactors, ViewPort viewPort, QueryMethod queryMethod) {
        List<TimeSeriesSpan> timeSeriesSpans = null;
        int[] numberOfGroups = new int[aggFactors.length];
        long[] aggregateIntervals = new long[aggFactors.length];
        for(int i = 0; i < aggFactors.length; i++) {
            numberOfGroups[i] = aggFactors[i] * viewPort.getWidth();
            aggregateIntervals[i] = (to - from) / numberOfGroups[i];
        }
        AggregatedDataPoints missingDataPoints = null;
        LOG.info("Fetching missing data from data source");
        missingDataPoints = dataSource.getAggregatedDataPoints(from, to, missingIntervals, measures, queryMethod, numberOfGroups);
        LOG.info("Fetched missing data from data source");
        timeSeriesSpans = TimeSeriesSpanFactory.createAggregate(missingDataPoints, missingIntervals, measures, aggregateIntervals, viewPort.getWidth());
        // Add the data points fetched from the data store to the pixel columns
//        missingDataPointList.forEach(aggregatedDataPoint -> {
//            addAggregatedDataPointToPixelColumns(from, to, viewPort, pixelColumns, aggregatedDataPoint);
//        });
        return timeSeriesSpans;
    }

    private int getPixelColumnForTimestamp(long timestamp, long from, long to, int width) {
        long aggregateInterval = (to - from) / width;
        return (int) ((timestamp - from) / aggregateInterval);
    }

    private void addAggregatedDataPointToPixelColumns(long from, long to, ViewPort viewPort, List<PixelColumn> pixelColumns, AggregatedDataPoint aggregatedDataPoint) {
        int pixelColumnIndex = getPixelColumnForTimestamp(aggregatedDataPoint.getFrom(), from, to, viewPort.getWidth());
        if (pixelColumnIndex < viewPort.getWidth()) {
            pixelColumns.get(pixelColumnIndex).addAggregatedDataPoint(aggregatedDataPoint);
        }
        // Since we only consider spans with intervals smaller than the pixel column interval, we know that the data point will not overlap more than two pixel columns.
        if (pixelColumnIndex <  viewPort.getWidth() - 1 && pixelColumns.get(pixelColumnIndex + 1).overlaps(aggregatedDataPoint)) {
            // If the next pixel column overlaps the data point, then we need to add the data point to the next pixel column as well.
            pixelColumns.get(pixelColumnIndex + 1).addAggregatedDataPoint(aggregatedDataPoint);
        }
    }

    private void addDataPointToPixelColumns(long from, long to, ViewPort viewPort, List<PixelColumn> pixelColumns, UnivariateDataPoint dataPoint){
        int pixelColumnIndex = getPixelColumnForTimestamp(dataPoint.getTimestamp(), from, to, viewPort.getWidth());
        if (pixelColumnIndex < viewPort.getWidth()) {
            pixelColumns.get(pixelColumnIndex).addDataPoint(dataPoint);
        }
    }

    public int getDataReductionRatio() {
        return dataReductionRatio;
    }
}
