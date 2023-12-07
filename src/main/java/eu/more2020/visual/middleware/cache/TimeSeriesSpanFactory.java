package eu.more2020.visual.middleware.cache;

import eu.more2020.visual.middleware.domain.*;
import eu.more2020.visual.middleware.util.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TimeSeriesSpanFactory {

    private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesSpanFactory.class);

    /**
     * Read from iterators and create raw time series spans.
     * All spans take account of the residual interval left from a not exact division with the aggregate interval.
     * The raw time series span needs to first collect the raw datapoints and then be built. This is because the sampling interval may vary.
     * @param dataPoints fetched raw datapoints
     * @param missingIntervalsPerMeasure  list of ranges for each measure that this points belong to
     * @return A list of RawTimeSeriesSpan for each measure
     */
    public static Map<Integer, List<TimeSeriesSpan>> createRaw(DataPoints dataPoints, Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure){
        Map<Integer, List<TimeSeriesSpan>> spans = new HashMap<>();
        Iterator<DataPoint> it = dataPoints.iterator();
        DataPoint dataPoint = null;
        for (Integer measure : missingIntervalsPerMeasure.keySet()) {
            List<TimeSeriesSpan> timeSeriesSpansForMeasure = new ArrayList<>();
            boolean changed = false;
            for(TimeInterval range : missingIntervalsPerMeasure.get(measure)) {
                RawTimeSeriesSpan timeSeriesSpan = new RawTimeSeriesSpan(range.getFrom(), range.getTo(), measure);
                List<DataPoint> dataPointsList = new ArrayList<>();
                while (it.hasNext()) {
                    if (!changed) dataPoint = it.next();
                    else changed = false;
                    if (dataPoint.getTimestamp() >= range.getTo()) {
                        changed = true;
                        break;
                    }
                    LOG.debug("Adding {} between {}-{}", dataPoint.getTimestamp(), range.getFrom(), range.getTo());
                    dataPointsList.add(dataPoint);
                }
                timeSeriesSpan.build(dataPointsList);
                timeSeriesSpansForMeasure.add(timeSeriesSpan);
                LOG.info("Created raw time series span:" + timeSeriesSpan);
            }
            spans.put(measure, timeSeriesSpansForMeasure);
        }
        return spans;
    }

    /**
     * Read from iterators and create time series spans.
     * All spans take account of the residual interval left from a not exact division with the aggregate interval.
     * @param aggregatedDataPoints fetched aggregated data points
     * @param missingIntervalsPerMeasure  list of ranges for each measure that this points belong to
     * @param aggregateIntervalsPerMeasure aggregate intervals with which to fetch data for each measure
     * @return A list of AggregateTimeSeriesSpan for each measure
     */
    public static Map<Integer, List<TimeSeriesSpan>> createAggregate(AggregatedDataPoints aggregatedDataPoints,
                                                                     Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
                                                                     Map<Integer, Long> aggregateIntervalsPerMeasure){
        Map<Integer, List<TimeSeriesSpan>> spans = new HashMap<>();
        Iterator<AggregatedDataPoint> it = aggregatedDataPoints.iterator();
        AggregatedDataPoint aggregatedDataPoint = null;
        for (Integer measure : missingIntervalsPerMeasure.keySet()) {
            List<TimeSeriesSpan> timeSeriesSpansForMeasure = new ArrayList<>();
            long aggregateInterval = aggregateIntervalsPerMeasure.get(measure);
            for(TimeInterval range : missingIntervalsPerMeasure.get(measure)) {
                AggregateTimeSeriesSpan timeSeriesSpan = new AggregateTimeSeriesSpan(range.getFrom(), range.getTo(), measure, aggregateInterval);
                int j = 0;
                while((j < timeSeriesSpan.getSize() - 1) && it.hasNext()){
                    aggregatedDataPoint = it.next();
                    j = DateTimeUtil.indexInInterval(range.getFrom(), range.getTo(), aggregateInterval, aggregatedDataPoint.getTimestamp());
                    LOG.debug("Adding {} between {}-{} with aggregate interval {} for measure {} at position {}",
                           aggregatedDataPoint.getTimestamp(), range.getFrom(), range.getTo(),  aggregateInterval, measure, j);
                    timeSeriesSpan.addAggregatedDataPoint(j, aggregatedDataPoint);
                }
                timeSeriesSpansForMeasure.add(timeSeriesSpan);
                LOG.debug("Created aggregate time series span:" + timeSeriesSpan);
            }
            spans.put(measure, timeSeriesSpansForMeasure);
        }
        return spans;
    }


}
