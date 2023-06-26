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


//   public static TimeSeriesSpan createFromRaw(DataPoints dataPoints, AggregateInterval aggregateInterval) {
//        TimeSeriesSpan timeSeriesSpan = new TimeSeriesSpan(dataPoints, aggregateInterval);
//        TimeAggregator timeAggregator = new TimeAggregator(dataPoints, aggregateInterval);
//        timeAggregator.getCount();
//        int i = 0;
//        AggregatedDataPoint aggregatedDataPoint;
//        while (timeAggregator.hasNext()) {
//            aggregatedDataPoint = timeAggregator.next();
//            if (i == 0) {
//                timeSeriesSpan.setFrom(aggregatedDataPoint.getTimestamp());
//            }
//            timeSeriesSpan.addAggregatedDataPoint(i, aggregatedDataPoint);
//            i++;
//        }
//        return timeSeriesSpan;
//    }


    public static List<TimeSeriesSpan> create(List<AggregatedDataPoint> aggregatedDataPointsList, List<Integer> measures,
                                              List<TimeInterval> ranges, int spanSize){
        List<TimeSeriesSpan> spans = new ArrayList<>();
        Iterator<AggregatedDataPoint> it = aggregatedDataPointsList.iterator();
        boolean changed = false;
        AggregatedDataPoint aggregatedDataPoint = null;

        for (TimeInterval range : ranges) {
            AggregateTimeSeriesSpan timeSeriesSpan = new AggregateTimeSeriesSpan(range.getFrom(), range.getTo(),
                    measures, spanSize);
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
                if(i == spanSize) continue;
                timeSeriesSpan.addAggregatedDataPoint(i, aggregatedDataPoint);
            }
            spans.add(timeSeriesSpan);
            LOG.info("Created time series span:" + timeSeriesSpan);
        }
        return spans;
    }


}
