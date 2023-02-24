package eu.more2020.visual.domain;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * An object for computing aggregate statistics for multi-variate time series data points.
 *
 * @implNote This implementation is not thread safe. However, it is safe to use
 * {@link java.util.stream.Collectors#summarizingDouble(java.util.function.ToDoubleFunction)
 * Collectors.summarizingDouble()} on a parallel stream, because the parallel
 * implementation of {@link java.util.stream.Stream#collect Stream.collect()}
 * provides the necessary partitioning, isolation, and merging of results for
 * safe and efficient parallel execution.
 * @since 1.8
 */
public class StatsAggregator implements Consumer<DataPoint>, Stats {

    private List<Integer> measures;
    private int count = 0;
    private double[] sums;
    private double[] minValues;
    private long[] minTimestamps;
    private double[] maxValues;
    private long[] maxTimestamps;


    public StatsAggregator(List<Integer> measures) {
        this.measures = measures;
        int length = measures.size();
        sums = new double[length];
        minValues = new double[length];
        minTimestamps = new long[length];
        maxValues = new double[length];
        maxTimestamps = new long[length];

        Arrays.fill(minValues, Double.POSITIVE_INFINITY);
        Arrays.fill(maxValues, Double.NEGATIVE_INFINITY);
    }

    public void clear() {
        count = 0;
        Arrays.fill(sums, 0d);
        Arrays.fill(minValues, Double.POSITIVE_INFINITY);
        Arrays.fill(minTimestamps, -1l);
        Arrays.fill(maxValues, Double.NEGATIVE_INFINITY);
        Arrays.fill(maxTimestamps, -1l);
    }


    /**
     * Adds another datapoint into the summary information.
     *
     * @param dataPoint the dataPoint
     */
    @Override
    public void accept(DataPoint dataPoint) {
        ++count;
        for (int i = 0; i < dataPoint.getValues().length; i++) {
            double value = dataPoint.getValues()[i];
            sums[i] += value;
            minValues[i] = Math.min(minValues[i], value);
            if (minValues[i] == value) {
                minTimestamps[i] = dataPoint.getTimestamp();
            }
            maxValues[i] = Math.max(maxValues[i], value);
            if (maxValues[i] == value) {
                maxTimestamps[i] = dataPoint.getTimestamp();
            }
        }
    }

    public void accept(AggregatedDataPoint dataPoint) {
        Stats stats = dataPoint.getStats();
        if (stats.getCount() != 0) {
            count += stats.getCount();
            int i = 0;
            for (int m : measures) {
                sums[i] += stats.getSum(m);
                minValues[i] = Math.min(minValues[i], stats.getMinValue(m));
                if (minValues[i] == stats.getMinValue(m)) {
                    minTimestamps[i] = stats.getMinTimestamp(m);
                }
                maxValues[i] = Math.max(maxValues[i], stats.getMaxValue(m));
                if (maxValues[i] == stats.getMaxValue(m)) {
                    maxTimestamps[i] = stats.getMaxTimestamp(m);
                }
                i++;
            }
        }
    }

    /**
     * Combines the state of another {@code StatsAggregator} instance into this
     * one.
     *
     * @param other another {@code StatsAggregator}
     * @throws NullPointerException if {@code other} is null
     */
    public void combine(StatsAggregator other) {
        count += other.count;
        for (int i = 0; i < sums.length; i++) {
            sums[i] += other.sums[i];
            minValues[i] = Math.min(minValues[i], other.minValues[i]);
            if (minValues[i] == other.minValues[i]) {
                minTimestamps[i] = other.minTimestamps[i];
            }
            maxValues[i] = Math.max(maxValues[i], other.maxValues[i]);
            if (maxValues[i] == other.maxValues[i]) {
                maxTimestamps[i] = other.maxTimestamps[i];
            }
        }
    }

    @Override
    public int getCount() {
        return count;
    }

    @Override
    public double getSum(int measure) {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return sums[getMeasureIndex(measure)];
    }

    @Override
    public double getMinValue(int measure) {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return minValues[getMeasureIndex(measure)];
    }

    @Override
    public double getMaxValue(int measure) {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return maxValues[getMeasureIndex(measure)];
    }

    @Override
    public double getAverageValue(int measure) {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return sums[getMeasureIndex(measure)] / count;
    }

    @Override
    public long getMinTimestamp(int measure) {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return minTimestamps[getMeasureIndex(measure)];
    }

    @Override
    public long getMaxTimestamp(int measure) {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return maxTimestamps[getMeasureIndex(measure)];    }

    private int getMeasureIndex(int measure) {
        return measures.indexOf(measure);
    }

}
