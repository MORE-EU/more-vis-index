package eu.more2020.visual.domain;

import java.io.Serializable;
import java.time.ZonedDateTime;
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
public class PixelStatsAggregator extends StatsAggregator {

    private final double[] firstValues;
    private final long[] firstTimestamps;
    private final double[] lastValues;
    private final long[] lastTimestamps;

    private int[] minId;
    private int[] trueMinId;

    private int[] maxId;
    private int[] trueMaxId;

    private int height;
    private StatsAggregator stats;
    public PixelStatsAggregator(List<Integer> measures) {
        super(measures);

        int length = measures.size();
        firstValues = new double[length];
        firstTimestamps = new long[length];
        lastValues = new double[length];
        lastTimestamps = new long[length];

        Arrays.fill(firstValues, Double.NEGATIVE_INFINITY);
        Arrays.fill(firstTimestamps, Long.MAX_VALUE);
        Arrays.fill(lastValues, Double.POSITIVE_INFINITY);
        Arrays.fill(lastTimestamps, Long.MIN_VALUE);
    }

    public PixelStatsAggregator(StatsAggregator stats, List<Integer> measures, ViewPort viewPort) {
        this(measures);
        this.stats = stats;
        this.height = viewPort.getHeight();

        minId = new int[height];
        maxId = new int[height];
        trueMinId = new int[height];
        trueMaxId = new int[height];
        Arrays.fill(minId, 0);
        Arrays.fill(trueMinId, 0);
        Arrays.fill(maxId,height - 1);
        Arrays.fill(trueMaxId, height - 1);
    }

    @Override
    public void accept(AggregatedDataPoint dataPoint) {
        Stats stats = dataPoint.getStats();
        if (stats.getCount() != 0) {
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
                if (count == 0) {
                    if (stats.getMinTimestamp(m) <= stats.getMaxTimestamp(m)) {
                        firstTimestamps[i] = Math.min(stats.getMinTimestamp(m), firstTimestamps[i]);
                        if(firstTimestamps[i] == stats.getMinTimestamp(m)) {
                            firstValues[i] = stats.getMinValue(m);
                        }
                    } else {
                        firstTimestamps[i] = Math.min(stats.getMaxTimestamp(m), firstTimestamps[i]);
                        if(firstTimestamps[i] == stats.getMaxTimestamp(m)) {
                            firstValues[i] = stats.getMaxValue(m);
                        }
                    }
                }
                if (stats.getMinTimestamp(m) <= stats.getMaxTimestamp(m)) {
                    lastTimestamps[i] = Math.max(stats.getMaxTimestamp(m), lastTimestamps[i]);
                    if(lastTimestamps[i] == stats.getMaxTimestamp(m)) {
                        lastValues[i] = stats.getMaxValue(m);
                    }
                } else {
                    lastTimestamps[i] = Math.max(stats.getMinTimestamp(m), lastTimestamps[i]);
                    if(lastTimestamps[i] == stats.getMinTimestamp(m)) {
                        lastValues[i] = stats.getMinValue(m);
                    }
                }
                i++;
            }
            count += 1;
        }
    }

    /** Accepts a partially overlapping interval and computes the inner-column errors.
        @param from is the start of the interval
        @param to is the end of the interval
        @param isLast signifies if the interval is to the right or to the left of a pixelColumn
     **/
    public void accept(AggregatedDataPoint dataPoint, ZonedDateTime from, ZonedDateTime to, boolean isLast) {
        Stats stats = dataPoint.getStats();
        if (stats.getCount() != 0) {
            int i = 0;
            for (int m : measures) {
                sums[i] += stats.getSum(m);
                // Contains min
                if(from.toInstant().toEpochMilli() <= stats.getMinTimestamp(m) && to.toInstant().toEpochMilli() >= stats.getMinTimestamp(m)){
                    minValues[i] = Math.min(minValues[i], stats.getMinValue(m));
                    minId[i] = getPixelId(m, minValues[i]);
                    if (minValues[i] == stats.getMinValue(m)) {
                        minTimestamps[i] = stats.getMinTimestamp(m);
                    }
                    if(isLast){
                        if(stats.getMinTimestamp(m) >= lastTimestamps[i]){
                            lastTimestamps[i] = stats.getMinTimestamp(m);
                            lastValues[i] = stats.getMinValue(m);
                        }
                    } else {
                        if(stats.getMinTimestamp(m) <= firstTimestamps[i]){
                            firstTimestamps[i] = stats.getMinTimestamp(m);
                            firstValues[i] = stats.getMaxValue(m);
                        }
                    }
                    minId[i] = trueMinId[i] = getPixelId(m, minValues[i]);
                    trueMaxId[i] = Math.min(getPixelId(m, stats.getMaxValue(m)), trueMaxId[i]);
                }
                // Contains max
                if(from.toInstant().toEpochMilli() <= stats.getMaxTimestamp(m) && to.toInstant().toEpochMilli() >= stats.getMaxTimestamp(m)){
                    maxValues[i] = Math.max(maxValues[i], stats.getMaxValue(m));
                    maxId[i] = getPixelId(m, maxValues[i]);
                    if (maxValues[i] == stats.getMaxValue(m)) {
                        maxTimestamps[i] = stats.getMaxTimestamp(m);
                    }
                    if(isLast){
                        if(stats.getMaxTimestamp(m) >= lastTimestamps[i]){
                            lastTimestamps[i] = stats.getMaxTimestamp(m);
                            lastValues[i] = stats.getMaxValue(m);
                        }
                    } else {
                        if(stats.getMaxTimestamp(m) <= firstTimestamps[i]){
                            firstTimestamps[i] = stats.getMaxTimestamp(m);
                            firstValues[i] = stats.getMaxValue(m);
                        }
                    }
                    maxId[i] = trueMaxId[i] = getPixelId(m, maxValues[i]);
                    trueMinId[i] = Math.max(getPixelId(m, stats.getMinValue(m)), trueMinId[i]);
                }
                i ++;
            }
        }
    }

    public int getPixelId(int m, double value){
        double range = Math.abs(stats.getMaxValue(m)) + Math.abs(stats.getMinValue(m));
        double bin_size = range / height;
        return (int) ((Math.abs(value) / bin_size));
    }

    @Override
    public PixelStatsAggregator clone(){
        PixelStatsAggregator statsAggregator = new PixelStatsAggregator(measures);
        statsAggregator.combine(this);
        return statsAggregator;
    }

    @Override
    public void clear() {
        count = 0;
        Arrays.fill(sums, 0d);
        Arrays.fill(minValues, Double.POSITIVE_INFINITY);
        Arrays.fill(minTimestamps, -1l);
        Arrays.fill(maxValues, Double.NEGATIVE_INFINITY);
        Arrays.fill(maxTimestamps, -1l);
        Arrays.fill(firstValues, Double.NEGATIVE_INFINITY);
        Arrays.fill(firstTimestamps, Long.MAX_VALUE);
        Arrays.fill(lastValues, Double.NEGATIVE_INFINITY);
        Arrays.fill(lastTimestamps, Long.MIN_VALUE);
    }


    @Override
    public void combine(StatsAggregator other) {
        PixelStatsAggregator otherP = (PixelStatsAggregator) other;
        count += otherP.count;
        for (int i = 0; i < sums.length; i++) {
            sums[i] += otherP.sums[i];
            minValues[i] = Math.min(minValues[i], otherP.minValues[i]);
            if (minValues[i] == otherP.minValues[i]) {
                minTimestamps[i] = otherP.minTimestamps[i];
            }
            maxValues[i] = Math.max(maxValues[i], otherP.maxValues[i]);
            if (maxValues[i] == otherP.maxValues[i]) {
                maxTimestamps[i] = otherP.maxTimestamps[i];
            }
            firstValues[i] = otherP.firstValues[i];
            firstTimestamps[i] = otherP.firstTimestamps[i];
            lastValues[i] = otherP.lastValues[i];
            lastTimestamps[i] = otherP.lastTimestamps[i];
        }
    }

    public double getFirstValue(int measure) {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return firstValues[getMeasureIndex(measure)];
    }

    public double getLastValue(int measure) {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return lastValues[getMeasureIndex(measure)];
    }

    public long getFirstTimestamp(int measure) {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return firstTimestamps[getMeasureIndex(measure)];
    }


    public long getLastTimestamp(int measure) {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return lastTimestamps[getMeasureIndex(measure)];
    }


    public long getMinPixelId(int measure) {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return minId[getMeasureIndex(measure)];
    }

    public long getTrueMinPixelId(int measure) {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return trueMinId[getMeasureIndex(measure)];
    }

    public long getMaxPixelId(int measure) {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return maxId[getMeasureIndex(measure)];
    }


    public long getTrueMaxPixelId(int measure) {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return trueMaxId[getMeasureIndex(measure)];
    }



    }