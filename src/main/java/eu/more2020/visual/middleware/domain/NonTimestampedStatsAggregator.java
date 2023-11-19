package eu.more2020.visual.middleware.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * An implementation of the Stats interface that calculates stats within a fixed interval [from, to).
 * This class only takes into account values and not timestamps of data points.
 * As timestamps the middle of the interval is used for the data points with the min and max values,
 * and the from and to timestamps for the first and last data points.
 */
public class NonTimestampedStatsAggregator implements Stats, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(StatsAggregator.class);

    private List<Integer> measures;
    private  long from;
    private long to;

    private int count = 0;
    private final double[] sums;
    private final double[] minValues;
    private final double[] maxValues;

    public NonTimestampedStatsAggregator(List<Integer> measures) {
        this.measures = measures;
        int length = measures.size();
        sums = new double[length];
        minValues = new double[length];
        maxValues = new double[length];
        clear();
    }

    public void clear() {
        count = 0;
        Arrays.fill(sums, 0d);
        Arrays.fill(minValues, Double.POSITIVE_INFINITY);
        Arrays.fill(maxValues, Double.NEGATIVE_INFINITY);
    }

    public void accept(double value, int measure) {
        ++count;
        int i = getMeasureIndex(measure);
        sums[i] += value;
        minValues[i] = Math.min(minValues[i], value);
        maxValues[i] = Math.max(maxValues[i], value);
    }

    /**
     * Combines the state of a {@code Stats} instance into this
     * StatsAggregator.
     *
     * @param other another {@code Stats}
     * Handles the case of different measures
     */
    public void combine(Stats other) {
        if(other.getCount() != 0) {
            for (int m : other.getMeasures()) {
                int i = getMeasureIndex(m);
                sums[i] += other.getSum(m);
                minValues[i] = Math.min(minValues[i], other.getMinValue(m));
                maxValues[i] = Math.max(maxValues[i], other.getMaxValue(m));
            }
            count += other.getCount();
        }
    }

    public List<Integer> getMeasures() {
        return measures;
    }

    public int getCount() {
        return count;
    }

    public int getCount(int measure) {
        return count;
    }

    public double getSum(int measure) {
        return sums[getMeasureIndex(measure)];
    }

    public double getMinValue(int measure) {
        return minValues[getMeasureIndex(measure)];
    }

    @Override
    public long getMinTimestamp(int measure) {
        return (from + to) / 2;
    }

    public double getMaxValue(int measure) {
        return maxValues[getMeasureIndex(measure)];
    }

    @Override
    public long getMaxTimestamp(int measure) {
        return (from + to) / 2;
    }

    @Override
    public double getFirstValue(int measure) {
        return (getMinValue(measure) + getMaxValue(measure)) / 2;
    }

    @Override
    public long getFirstTimestamp(int measure) {
        return from + 1;
    }

    @Override
    public double getLastValue(int measure) {
        return (getMinValue(measure) + getMaxValue(measure)) / 2;
    }

    @Override
    public long getLastTimestamp(int measure) {
        return to - 1;
    }

    @Override
    public double getAverageValue(int measure) {
        return getSum(measure) / getCount();
    }

    protected int getMeasureIndex(int measure) {
        return measures.indexOf(measure);
    }

    public void setFrom(long from) {
        this.from = from;
    }

    public void setTo(long to) {
        this.to = to;
    }
}
