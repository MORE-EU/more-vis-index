package eu.more2020.visual.domain;


import java.util.List;

/**
 * Represents a sequence of data points that can be traversed in time-ascending order
 */
public interface DataPoints extends Iterable<DataPoint>, TimeInterval {

    /**
     * Returns the measures included in the data points, in the exact order that are returned from {@link DataPoint#getValues()}
     */
    public List<Integer> getMeasures();


}