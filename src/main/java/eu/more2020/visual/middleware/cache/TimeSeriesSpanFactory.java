package eu.more2020.visual.middleware.cache;

import eu.more2020.visual.middleware.domain.*;
import eu.more2020.visual.middleware.util.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TimeSeriesSpanFactory {

    private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesSpanFactory.class);

    public static List<TimeSeriesSpan> createRaw(List<DataPoint> dataPointsList, List<TimeInterval> ranges, List<List<Integer>> measures){
        List<TimeSeriesSpan> spans = new ArrayList<>();
        Iterator<DataPoint> it = dataPointsList.iterator();
        DataPoint dataPoint = null;
        int i = 0;
//        for (TimeInterval range : ranges) {
//            RawTimeSeriesSpan timeSeriesSpan = new RawTimeSeriesSpan(range.getFrom(), range.getTo());
//            List<DataPoint> dataPoints = new ArrayList<>();
//            while(it.hasNext()){
//                dataPoint = it.next();
//                if (dataPoint.getTimestamp() >= range.getTo()) {
//                    break;
//                }
//                LOG.debug("Adding {} between {}-{}", dataPoint.getTimestamp(), range.getFrom(), range.getTo());
//                dataPoints.add(dataPoint);
//            }
//            timeSeriesSpan.build(dataPoints);
//            spans.add(timeSeriesSpan);
//            LOG.info("Created raw time series span:" + timeSeriesSpan);
//            i ++;
//        }
        return spans;
    }

    /**
     * Read from iterators and create time series spans.
     * All spans take account of the residual interval left from a not exact division with the aggregate interval.
     * @param aggregatedDataPoints fetched aggredgated datapoints
     * @param measures list of measures
     * @param ranges  list of ranges for each measure that this points belong to
     * @param aggregateIntervals
     * @return
     */
    public static List<TimeSeriesSpan> createAggregate(AggregatedDataPoints aggregatedDataPoints, List<List<TimeInterval>> ranges,
                                                       List<Integer> measures, long[] aggregateIntervals, int width){
        List<TimeSeriesSpan> spans = new ArrayList<>();
        Iterator<AggregatedDataPoint> it = aggregatedDataPoints.iterator();
        boolean changed = false;
        AggregatedDataPoint aggregatedDataPoint = null;
        int i = 0;
//        for (TimeInterval range : ranges) {
//            AggregateTimeSeriesSpan timeSeriesSpan = new AggregateTimeSeriesSpan(range.getFrom(), range.getTo(),  measures.get(i), aggregateInterval, width);
//            while(it.hasNext()){
//                if(!changed) aggregatedDataPoint = it.next();
//                else changed = false;
//                if ((aggregatedDataPoint.getTimestamp() >= range.getTo()) || !aggregatedDataPoint.getStats().measuresEqual(measures.get(i))) {
//                    changed = true;
//                    break;
//                }
//                int j = DateTimeUtil.indexInInterval(range.getFrom(), range.getTo(), aggregateInterval, aggregatedDataPoint.getTimestamp());
//                LOG.debug("Adding {} between {}-{} with aggregate interval {} at position {}", aggregatedDataPoint.getTimestamp(), range.getFrom(), range.getTo(), aggregateInterval, j);
//                timeSeriesSpan.addAggregatedDataPoint(j, aggregatedDataPoint);
//            }
//            spans.add(timeSeriesSpan);
//            i ++;
//            LOG.debug("Created aggregate time series span:" + timeSeriesSpan);
//        }
        return spans;
    }


}
