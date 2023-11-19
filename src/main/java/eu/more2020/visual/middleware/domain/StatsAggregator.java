package eu.more2020.visual.middleware.domain;

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
    protected int count;
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
        count = 0;
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
        for (int measure : measures) {
            int i = getMeasureIndex(measure);
            if(i == - 1) continue; // check if datapoint has this measure
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
            count ++;
        }
    }

    public void accept(UnivariateDataPoint dataPoint, int measure) {
        double value = dataPoint.getValue();
        int i = getMeasureIndex(measure);

        if (minValues[i] > value) {
            minValues[i] = value;
            minTimestamps[i] = dataPoint.getTimestamp();
        }

        if (maxValues[i] < value) {
            maxValues[i] = value;
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
        count ++;
    }


    public void accept(AggregatedDataPoint dataPoint) {
        Stats stats = dataPoint.getStats();
        if (dataPoint.getCount() != 0) {
            for (int m : measures) {
                int i = getMeasureIndex(m);
                if(i == - 1) continue; // check if datapoint has this measure
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
                count += dataPoint.getCount();
            }
        }
    }

    /**
     * Combines the state of a {@code Stats} instance into this
     * StatsAggregator.
     *
     * @param other another {@code Stats}
     * @throws IllegalArgumentException if the other Stats instance does not have the same measures as this StatsAggregator
     */
    public void combine(Stats other) {
        if (!hasSameMeasures(other)) {
            throw new IllegalArgumentException("Cannot combine stats with different measures");
        }
        if(other.getCount() != 0) {
            for (int m : measures) {
                int i = getMeasureIndex(m);
                sums[i] += other.getSum(m);
                minValues[i] = Math.min(minValues[i], other.getMinValue(m));
                if (minValues[i] == other.getMinValue(m)) {
                    minTimestamps[i] = other.getMinTimestamp(m);
                }
                maxValues[i] = Math.max(maxValues[i], other.getMaxValue(m));
                if (maxValues[i] == other.getMaxValue(m)) {
                    maxTimestamps[i] = other.getMaxTimestamp(m);
                }
                if (firstTimestamps[i] > other.getFirstTimestamp(m)) {
                    firstValues[i] = other.getFirstValue(m);
                    firstTimestamps[i] = other.getFirstTimestamp(m);
                }
                if (lastTimestamps[i] < other.getLastTimestamp(m)) {
                    lastValues[i] = other.getLastValue(m);
                    lastTimestamps[i] = other.getLastTimestamp(m);
                }
            }
            count += other.getCount();
        }
    }


    @Override
    public List<Integer> getMeasures() {
        return measures;
    }

    @Override
    public int getCount() {
        return count;
    }

    @Override
    public int getCount(int measure) {
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
