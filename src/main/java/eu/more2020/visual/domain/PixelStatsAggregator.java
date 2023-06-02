package eu.more2020.visual.domain;

import eu.more2020.visual.util.DateTimeUtil;

import java.io.Serializable;
import java.time.ZoneId;
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
public class PixelStatsAggregator extends SubPixelStatsAggregator {

    private final double[] firstValues;
    private final long[] firstTimestamps;
    private final double[] lastValues;
    private final long[] lastTimestamps;

    private int[] minId;
    private int[] trueMinId;

    private int[] maxId;
    private int[] trueMaxId;

    private int height;
    private StatsAggregator globalStats = null;

    /**
     * The start date time value of the current pixel.
     */
    private ZonedDateTime currentPixel;
    private ZonedDateTime nextPixel;
    private AggregateInterval m4Interval;

    private PixelStatsAggregator(List<Integer> measures) {
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

    public PixelStatsAggregator(StatsAggregator globalStats, ZonedDateTime firstPixel,
                                AggregateInterval m4Interval, List<Integer> measures, ViewPort viewPort) {
        this(measures);
        this.globalStats = globalStats;
        this.height = viewPort.getHeight();

        minId = new int[height];
        maxId = new int[height];
        trueMinId = new int[height];
        trueMaxId = new int[height];
        Arrays.fill(minId, 0);
        Arrays.fill(trueMinId, 0);
        Arrays.fill(maxId,height - 1);
        Arrays.fill(trueMaxId, height - 1);
        currentPixel = firstPixel;
        nextPixel = currentPixel.plus(m4Interval.getInterval(), m4Interval.getChronoUnit());
        this.m4Interval = m4Interval;
    }


    @Override
    public void accept(AggregatedDataPoint dataPoint) {
        Stats stats = dataPoint.getStats();
        if (stats.getCount() != 0) {
            int i = 0;
            for (int m : measures) {
                sums[i] += stats.getSum(m);
                trueMinId[i] = Math.min(getPixelId(m, stats.getMinValue(m)), minId[i]);
                trueMaxId[i] = Math.max(getPixelId(m, stats.getMaxValue(m)), maxId[i]);
                // Contains min
                if(currentPixel.toInstant().toEpochMilli() <= stats.getMinTimestamp(m)
                        && nextPixel.toInstant().toEpochMilli() >= stats.getMinTimestamp(m)){
                    minValues[i] = Math.min(minValues[i], stats.getMinValue(m));
                    if (minValues[i] == stats.getMinValue(m)) {
                        minTimestamps[i] = stats.getMinTimestamp(m);
                    }
                    minId[i] = trueMinId[i] = getPixelId(m, minValues[i]);
                    maxValues[i] = Math.max(maxValues[i], stats.getMinValue(m));
                    if (maxValues[i] == stats.getMinValue(m)) {
                        maxTimestamps[i] = stats.getMinTimestamp(m);
                    }
                    if(count == 0){
                        if(stats.getMinTimestamp(m) <= firstTimestamps[i]){
                            firstTimestamps[i] = stats.getMinTimestamp(m);
                            firstValues[i] = stats.getMinValue(m);
                        }
                    }
                    if(stats.getMinTimestamp(m) >= lastTimestamps[i]){
                        lastTimestamps[i] = stats.getMinTimestamp(m);
                        lastValues[i] = stats.getMinValue(m);
                    }
                }
                // Contains max
                if(currentPixel.toInstant().toEpochMilli() <= stats.getMaxTimestamp(m)
                        && nextPixel.toInstant().toEpochMilli() >= stats.getMaxTimestamp(m)){
                    maxValues[i] = Math.max(maxValues[i], stats.getMaxValue(m));
                    if (maxValues[i] == stats.getMaxValue(m)) {
                        maxTimestamps[i] = stats.getMaxTimestamp(m);
                    }
                    maxId[i] = trueMaxId[i] = getPixelId(m, maxValues[i]);
                    minValues[i] = Math.min(minValues[i], stats.getMaxValue(m));
                    if (minValues[i] == stats.getMaxValue(m)) {
                        minTimestamps[i] = stats.getMaxTimestamp(m);
                    }
                    if(count == 0){
                        if(stats.getMaxTimestamp(m) <= firstTimestamps[i]){
                            firstTimestamps[i] = stats.getMaxTimestamp(m);
                            firstValues[i] = stats.getMaxValue(m);
                        }
                    }
                    if(stats.getMaxTimestamp(m) >= lastTimestamps[i]){
                        lastTimestamps[i] = stats.getMaxTimestamp(m);
                        lastValues[i] = stats.getMaxValue(m);
                    }
                }
                i ++;
            }
            count ++;
        }
    }

    public int getPixelId(int m, double value){
        double range = Math.abs(globalStats.getMaxValue(m)) + Math.abs(globalStats.getMinValue(m));
        double bin_size = range / height;
        return (int) ((Math.abs(value) / bin_size));
    }

    @Override
    public PixelStatsAggregator clone(){
        PixelStatsAggregator statsAggregator = new PixelStatsAggregator(measures);
        statsAggregator.combine(this);
        return statsAggregator;
    }

    public void moveToNextPixel(){
        currentPixel = currentPixel.plus(m4Interval.getInterval(), m4Interval.getChronoUnit());
        nextPixel = currentPixel.plus(m4Interval.getInterval(), m4Interval.getChronoUnit());
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


    public int getMinPixelId(int measure) {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return minId[getMeasureIndex(measure)];
    }

    public int getTrueMinPixelId(int measure) {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return trueMinId[getMeasureIndex(measure)];
    }

    public int getMaxPixelId(int measure) {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return maxId[getMeasureIndex(measure)];
    }


    public int getTrueMaxPixelId(int measure) {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return trueMaxId[getMeasureIndex(measure)];
    }

    public int getFirstPixelId(int measure) {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return getPixelId(measure, firstValues[getMeasureIndex(measure)]);
    }

    public int getLastPixelId(int measure) {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return getPixelId(measure, lastValues[getMeasureIndex(measure)]);
    }

    public StatsAggregator getGlobalStats() {
        return globalStats;
    }

    public void movePixel(ZonedDateTime from, ZonedDateTime to) {
        currentPixel = from;
        nextPixel = to;
    }
}