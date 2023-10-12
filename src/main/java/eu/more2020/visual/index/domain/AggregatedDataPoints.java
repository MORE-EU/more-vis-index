package eu.more2020.visual.index.domain;

import java.util.List;

public interface AggregatedDataPoints extends Iterable<AggregatedDataPoint>, TimeInterval{
    List<Integer> getMeasures();
}
