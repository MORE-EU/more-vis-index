package eu.more2020.visual.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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
public class StatsAggregator implements Consumer<DataPoint>, Stats, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(StatsAggregator.class);

    protected List<Integer> measures;
    protected int count = 0;
    protected final double[] sums;
    protected final double[] minValues;
    protected final long[] minTimestamps;
    protected final double[] maxValues;
    protected final long[] maxTimestamps;
    protected final double[] firstValues;
    protected final long[] firstTimestamps;
    protected final double[] lastValues;
    protected final long[] lastTimestamps;

    public StatsAggregator(List<Integer> measures) {
        this.measures = measures;
        int length = measures.size();
        sums = new double[length];
        minValues = new double[length];
        minTimestamps = new long[length];
        maxValues = new double[length];
        maxTimestamps = new long[length];
        firstValues = new double[length];
        firstTimestamps = new long[length];
        lastValues = new double[length];
        lastTimestamps = new long[length];
        clear();
    }

    public void clear() {
        count = 0;
        Arrays.fill(sums, 0d);
        Arrays.fill(minValues, Double.POSITIVE_INFINITY);
        Arrays.fill(minTimestamps, -1l);
        Arrays.fill(maxValues, Double.NEGATIVE_INFINITY);
        Arrays.fill(maxTimestamps, -1l);
        Arrays.fill(firstTimestamps, Long.MAX_VALUE);
        Arrays.fill(lastTimestamps, -1l);
    }


    /**
     * Adds another datapoint into the summary information.
     *
     * @param dataPoint the dataPoint
     */
    @Override
    public void accept(DataPoint dataPoint) {
        if (dataPoint instanceof AggregatedDataPoint) {
            accept((AggregatedDataPoint) dataPoint);
            return;
        }
        ++count;
        for (int measure : measures) {
            int i = getMeasureIndex(measure);
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
            if (firstTimestamps[i] > dataPoint.getTimestamp()) {
                firstValues[i] = value;
                firstTimestamps[i] = dataPoint.getTimestamp();
            }
            if (lastTimestamps[i] < dataPoint.getTimestamp()) {
                lastValues[i] = value;
                lastTimestamps[i] = dataPoint.getTimestamp();
            }
        }
    }

    public void accept(UnivariateDataPoint dataPoint, int measure) {
        ++count;
        double value = dataPoint.getValue();
        int i = getMeasureIndex(measure);
        sums[i] += value;
        minValues[i] = Math.min(minValues[i], value);
        if (minValues[i] == value) {
            minTimestamps[i] = dataPoint.getTimestamp();
        }
        maxValues[i] = Math.max(maxValues[i], value);
        if (maxValues[i] == value) {
            maxTimestamps[i] = dataPoint.getTimestamp();
        }
        if (firstTimestamps[i] > dataPoint.getTimestamp()) {
            firstValues[i] = value;
            firstTimestamps[i] = dataPoint.getTimestamp();
        }
        if (lastTimestamps[i] < dataPoint.getTimestamp()) {
            lastValues[i] = value;
            lastTimestamps[i] = dataPoint.getTimestamp();
        }
    }

    public void accept(AggregatedDataPoint dataPoint) {
        Stats stats = dataPoint.getStats();
        if (dataPoint.getCount() != 0) {
            count += dataPoint.getCount();
            for (int m : measures) {
                int i = getMeasureIndex(m);
                sums[i] += stats.getSum(m);
                minValues[i] = Math.min(minValues[i], stats.getMinValue(m));
                if (minValues[i] == stats.getMinValue(m)) {
                    minTimestamps[i] = stats.getMinTimestamp(m);
                }
                maxValues[i] = Math.max(maxValues[i], stats.getMaxValue(m));
                if (maxValues[i] == stats.getMaxValue(m)) {
                    maxTimestamps[i] = stats.getMaxTimestamp(m);
                }
                if (firstTimestamps[i] > stats.getMinTimestamp(m)) {
                    firstValues[i] = stats.getMinValue(m);
                    firstTimestamps[i] = stats.getMinTimestamp(m);
                }
                if (lastTimestamps[i] < stats.getMaxTimestamp(m)) {
                    lastValues[i] = stats.getMaxValue(m);
                    lastTimestamps[i] = stats.getMaxTimestamp(m);
                }
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
        if (!hasSameMeasures(other)) {
            throw new IllegalArgumentException("Cannot combine stats with different measures");
        }
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
            if (firstTimestamps[i] > other.firstTimestamps[i]) {
                firstValues[i] = other.firstValues[i];
                firstTimestamps[i] = other.firstTimestamps[i];
            }
            if (lastTimestamps[i] < other.lastTimestamps[i]) {
                lastValues[i] = other.lastValues[i];
                lastTimestamps[i] = other.lastTimestamps[i];
            }
        }
    }

    /**
     * Checks if the measures of this StatsAggregator and another StatsAggregator are the same.
     *
     * @param other another StatsAggregator
     * @return boolean - returns true if measures are the same, else false
     */
    public boolean hasSameMeasures(StatsAggregator other) {
        return this.measures != null && other.measures != null &&
                this.measures.size() == other.measures.size() &&
                this.measures.stream().allMatch(measure -> other.measures.contains(measure));
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
        return maxTimestamps[getMeasureIndex(measure)];
    }

    @Override
    public double getFirstValue(int measure) {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return firstValues[getMeasureIndex(measure)];
    }

    @Override
    public long getFirstTimestamp(int measure) {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return firstTimestamps[getMeasureIndex(measure)];
    }

    @Override
    public double getLastValue(int measure) {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return lastValues[getMeasureIndex(measure)];
    }

    @Override
    public long getLastTimestamp(int measure) {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return lastTimestamps[getMeasureIndex(measure)];
    }

    protected int getMeasureIndex(int measure) {
        return measures.indexOf(measure);
    }

    public StatsAggregator clone() {
        StatsAggregator statsAggregator = new StatsAggregator(measures);
        statsAggregator.combine(this);
        return statsAggregator;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return count == 0 ? "count=0" :
                measures.stream()
                        .map(measure -> this.toString(measure))
                        .collect(Collectors.joining(", "));
    }
}
