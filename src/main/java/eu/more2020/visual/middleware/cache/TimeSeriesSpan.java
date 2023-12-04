package eu.more2020.visual.middleware.cache;
import eu.more2020.visual.middleware.domain.DataPoints;
import eu.more2020.visual.middleware.domain.TimeInterval;

import java.util.Iterator;
import java.util.List;

public interface TimeSeriesSpan extends TimeInterval, DataPoints {
    long getAggregateInterval();
    Iterator iterator(long from, long to);
    int[] getCounts();
    long calculateDeepMemorySize();
    List<Integer> getMeasures();
    int getWidth();
}
