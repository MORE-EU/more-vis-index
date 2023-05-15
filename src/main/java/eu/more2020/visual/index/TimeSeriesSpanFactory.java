package eu.more2020.visual.index;

import eu.more2020.visual.domain.*;
import eu.more2020.visual.util.DateTimeUtil;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TimeSeriesSpanFactory {

    //TODO: Handle first and last tuples for database results

    /**
     * @param dataPoints
     * @param aggregateInterval
     */
    public static TimeSeriesSpan createFromRaw(DataPoints dataPoints, AggregateInterval aggregateInterval) {
        TimeSeriesSpan timeSeriesSpan = new TimeSeriesSpan(dataPoints, aggregateInterval);
        TimeAggregator timeAggregator = new TimeAggregator(dataPoints, aggregateInterval);
        timeAggregator.getCount();
        int i = 0;
        AggregatedDataPoint aggregatedDataPoint;
        while (timeAggregator.hasNext()) {
            aggregatedDataPoint = timeAggregator.next();
            if (i == 0) {
                timeSeriesSpan.setFrom(aggregatedDataPoint.getTimestamp());
            }
            timeSeriesSpan.addAggregatedDataPoint(i, aggregatedDataPoint);
            i++;
        }
        return timeSeriesSpan;
    }

    public static List<TimeSeriesSpan> create(AggregatedDataPoints aggregatedDataPoints,
                                             List<TimeRange> ranges, AggregateInterval aggregateInterval){
        List<TimeSeriesSpan> spans = new ArrayList<>();
        Iterator<AggregatedDataPoint> it = aggregatedDataPoints.iterator();
        boolean changed = false;
        AggregatedDataPoint aggregatedDataPoint = null;
        for (TimeRange range : ranges) {
            TimeSeriesSpan timeSeriesSpan = new TimeSeriesSpan(range.getFrom(), range.getTo(),
                    aggregatedDataPoints.getMeasures(), aggregateInterval);
            int c = 0;
            int intervals = timeSeriesSpan.getSize();
            while(it.hasNext()){
                if(!changed) aggregatedDataPoint = it.next();
                else changed = false;
                if (aggregatedDataPoint.getTimestamp() > range.getTo()) {
                    changed = true;
                    break;
                }
                int i = DateTimeUtil.indexInInterval(range.getFrom(), range.getTo(), intervals, aggregatedDataPoint.getTimestamp());
                if (c == 0) {
                    timeSeriesSpan.setFrom(aggregatedDataPoint.getTimestamp());
                }
                c++;
                timeSeriesSpan.addAggregatedDataPoint(i, aggregatedDataPoint);
            }
            spans.add(timeSeriesSpan);
        }
        return spans;
    }
}
