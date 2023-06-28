package eu.more2020.visual.index;
import eu.more2020.visual.domain.DataPoints;
import eu.more2020.visual.domain.TimeInterval;

import java.util.Iterator;

public interface TimeSeriesSpan extends TimeInterval, DataPoints {
    long getAggregateInterval();
    Iterator iterator(long from, long to);
    int[] getCounts();
    long calculateDeepMemorySize();
}
