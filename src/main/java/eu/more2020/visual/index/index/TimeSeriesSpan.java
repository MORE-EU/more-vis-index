package eu.more2020.visual.index.index;
import eu.more2020.visual.index.domain.DataPoints;
import eu.more2020.visual.index.domain.TimeInterval;
import org.apache.arrow.flatbuf.Int;

import java.util.Iterator;
import java.util.List;

public interface TimeSeriesSpan extends TimeInterval, DataPoints {
    long getAggregateInterval();
    Iterator iterator(long from, long to);
    int[] getCounts();
    long calculateDeepMemorySize();
    List<Integer> getMeasures();
}
