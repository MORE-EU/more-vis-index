package eu.more2020.visual.index;

import eu.more2020.visual.domain.*;
import eu.more2020.visual.util.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TimeSeriesSpanFactory {

    private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesSpanFactory.class);

    public static List<TimeSeriesSpan> createRaw(List<DataPoint> dataPointsList, List<Integer> measures,
                                                       List<TimeInterval> ranges){
        List<TimeSeriesSpan> spans = new ArrayList<>();
        Iterator<DataPoint> it = dataPointsList.iterator();
        DataPoint dataPoint = null;

        for (TimeInterval range : ranges) {
            RawTimeSeriesSpan timeSeriesSpan = new RawTimeSeriesSpan(range.getFrom(), range.getTo(), measures);
            List<DataPoint> dataPoints = new ArrayList<>();
            while(it.hasNext()){
                dataPoint = it.next();
                if (dataPoint.getTimestamp() >= range.getTo()) {
                    break;
                }
//                LOG.info("Adding {} between {}-{}", dataPoint.getTimestamp(), range.getFrom(), range.getTo());
                dataPoints.add(dataPoint);
            }
            timeSeriesSpan.build(dataPoints);
            spans.add(timeSeriesSpan);
            LOG.info("Created raw time series span:" + timeSeriesSpan);
        }
        return spans;
    }


    public static List<TimeSeriesSpan> createAggregate(List<AggregatedDataPoint> aggregatedDataPointsList, List<Integer> measures,
                                              List<TimeInterval> ranges, long aggregateInterval){
        List<TimeSeriesSpan> spans = new ArrayList<>();
        Iterator<AggregatedDataPoint> it = aggregatedDataPointsList.iterator();
        boolean changed = false;
        AggregatedDataPoint aggregatedDataPoint = null;

        for (TimeInterval range : ranges) {
            AggregateTimeSeriesSpan timeSeriesSpan = new AggregateTimeSeriesSpan(range.getFrom(), range.getTo(),  measures, aggregateInterval);
            int intervals = timeSeriesSpan.getSize();
            while(it.hasNext()){
                if(!changed) aggregatedDataPoint = it.next();
                else changed = false;
                if (aggregatedDataPoint.getTimestamp() >= range.getTo()) {
                    changed = true;
                    break;
                }
                if(aggregatedDataPoint.getCount() == 0) continue;
                //LOG.info("Adding {} between {}-{} with agg {}", aggregatedDataPoint.getTimestamp(), range.getFrom(), range.getTo(), spanSize);
                int i = DateTimeUtil.indexInInterval(range.getFrom(), range.getTo(), intervals, aggregatedDataPoint.getTimestamp());
                if(i == timeSeriesSpan.getSize()) continue;
                timeSeriesSpan.addAggregatedDataPoint(i, aggregatedDataPoint);
            }
            spans.add(timeSeriesSpan);
            LOG.info("Created aggregate time series span:" + timeSeriesSpan);
        }
        return spans;
    }


}
