package eu.more2020.visual.index;

import eu.more2020.visual.domain.*;
import eu.more2020.visual.util.DateTimeUtil;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A {@link DataPoints} implementation that aggregates a series of consecutive
 * raw data points based on the specified aggregation interval.
 * For each aggregation interval included and for each measure we store 5 doubles,
 * i.e. the sum, min and max aggregate values, 2 longs corresponding to the timestamp of the min and max value, as well as the corresponding
 * non-missing value counts.
 */
public class TimeSeriesSpan implements DataPoints, TimeInterval {

    int[] measures;

    /**
     * The aggregate values for every window interval and for every measure.
     */
    private long[][] aggsByMeasure;

    /**
     * The number of raw time series points behind every data point included in this time series span.
     * When the time series span corresponds to raw, non aggregated data, this number is 1.
     */
    private int[] counts;

    // The start time value of the span
    private long from;
    // The size of this span, corresponding to the number of aggregated window intervals represented by it
    private int size;

    /**
     * The fixed window that raw data points are grouped by in this span.
     */
    private AggregateInterval aggregateInterval;

    /**
     * @param dataPoints
     * @param aggregateInterval
     */
    public void build(DataPoints dataPoints, AggregateInterval aggregateInterval) {
        this.aggregateInterval = aggregateInterval;
        size = DateTimeUtil.numberOfIntervals(dataPoints.getFrom(), dataPoints.getTo(), aggregateInterval, null);

        measures = dataPoints.getMeasures().stream().mapToInt(Integer::intValue).toArray();
        counts = new int[size];
        aggsByMeasure = new long[measures.length][size * 5];

        TimeAggregator timeAggregator = new TimeAggregator(dataPoints, aggregateInterval);
        int i = 0;
        AggregatedDataPoint aggregatedDataPoint;

        while (timeAggregator.hasNext()) {
            aggregatedDataPoint = timeAggregator.next();
            if (i == 0) {
                from = aggregatedDataPoint.getTimestamp();
            }
            addAggregatedDataPoint(i, dataPoints.getMeasures(), aggregatedDataPoint);
            i++;
        }
    }

    protected void addAggregatedDataPoint(int i, List<Integer> measures, AggregatedDataPoint aggregatedDataPoint) {
        Stats stats = aggregatedDataPoint.getStats();
        counts[i] = stats.getCount();
        if (stats.getCount() == 0) {
            return;
        }
        for (int j = 0; j < measures.size(); j++) {
            int m = measures.get(j);
            long[] data = aggsByMeasure[j];
            data[5 * i] = Double.doubleToRawLongBits(stats.getSum(m));
            data[5 * i + 1] = Double.doubleToRawLongBits(stats.getMinValue(m));
            data[5 * i + 2] = stats.getMinTimestamp(m);
            data[5 * i + 3] = Double.doubleToRawLongBits(stats.getMaxValue(m));
            data[5 * i + 4] = stats.getMaxTimestamp(m);
        }
    }

    /**
     * Finds the index in the span in which the given timestamp should be.
     *
     * @param timestamp A timestamp in milliseconds since epoch.
     * @return A positive index.
     */
    private int getIndex(final long timestamp) {
        int index = DateTimeUtil.numberOfIntervals(from, timestamp, aggregateInterval, ZoneId.of("UTC")) - 1;
        if (index >= size) {
            return size - 1;
        } else if (index < 0) {
            return 0;
        }
        return index;
    }

    public List<Integer> getMeasures() {
        return Arrays.stream(measures).boxed().collect(Collectors.toList());
    }


    public int getSize() {
        return size;
    }

    public AggregateInterval getAggregateInterval() {
        return aggregateInterval;
    }

    public Iterator<AggregatedDataPoint> iterator(long queryStartTimestamp, long queryEndTimestamp) {
        return new TimeSeriesSpanIterator(queryStartTimestamp, queryEndTimestamp);
    }

    public TimeRange getTimeRange() {
        return new TimeRange(getFrom(), getTo());
    }


    @Override
    public Iterator iterator() {
        return iterator(from, -1);
    }

    @Override
    public long getFrom() {
        return from;
    }

    @Override
    public long getTo() {
        return aggregateInterval.getChronoUnit()
                .addTo(ZonedDateTime.ofInstant(Instant.ofEpochMilli(from), ZoneId.of("UTC")), size * aggregateInterval.getInterval())
                .toInstant().toEpochMilli();
    }

    @Override
    public String getFromDate() {
        return Instant.ofEpochMilli(getFrom()).atZone(ZoneId.of("UTC"))
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    @Override
    public String getToDate() {
        return Instant.ofEpochMilli(getTo()).atZone(ZoneId.of("UTC"))
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    @Override
    public String toString() {
        return getFromDate() + " - " + getToDate() + " " + aggregateInterval;
    }

    public boolean hasData() {
        return getTo() == getFrom();
    }

    private int getMeasureIndex(int measure) {
        for (int i = 0; i < measures.length; i++) {
            if (measures[i] == measure) {
                return i;
            }
        }
        return -1;
    }


    /**
     * Calculates the deep memory size of this instance.
     *
     * @return The deep memory size in bytes.
     */
    public long calculateDeepMemorySize() {
        // Memory overhead for an object in a 64-bit JVM
        final int OBJECT_OVERHEAD = 16;
        // Memory overhead for an array in a 64-bit JVM
        final int ARRAY_OVERHEAD = 24;
        // Memory usage of int in a 64-bit JVM
        final int INT_SIZE = 4;
        // Memory usage of long in a 64-bit JVM
        final int LONG_SIZE = 8;
        // Memory usage of a reference in a 64-bit JVM with a heap size less than 32 GB
        final int REF_SIZE = 4;

        long measuresMemory = REF_SIZE + ARRAY_OVERHEAD + (measures.length * INT_SIZE);

        long aggsByMeasureMemory = REF_SIZE + ARRAY_OVERHEAD +
                aggsByMeasure.length * (REF_SIZE + ARRAY_OVERHEAD + (measures.length * LONG_SIZE));

        long countsMemory = REF_SIZE + ARRAY_OVERHEAD + (counts.length * INT_SIZE);

        long aggregateIntervalMemory = 2 * REF_SIZE + OBJECT_OVERHEAD + LONG_SIZE;

        long deepMemorySize = REF_SIZE + OBJECT_OVERHEAD + measuresMemory +
                aggsByMeasureMemory + countsMemory + LONG_SIZE + INT_SIZE + aggregateIntervalMemory;

        return deepMemorySize;
    }


    private class TimeSeriesSpanIterator implements Iterator<AggregatedDataPoint>, AggregatedDataPoint {

        private Iterator<Integer> internalIt;
        private ZonedDateTime startInterval;

        private long timestamp;

        private int currentIndex = -1;

        public TimeSeriesSpanIterator(long queryStartTimestamp, long queryEndTimestamp) {
            internalIt = IntStream.range(getIndex(queryStartTimestamp), queryEndTimestamp >= 0 ? getIndex(queryEndTimestamp) + 1 : size)
                    .iterator();
            startInterval = DateTimeUtil.getIntervalStart(from, aggregateInterval, ZoneId.of("UTC"));
        }

        @Override
        public boolean hasNext() {
            return internalIt.hasNext();
        }

        @Override
        public AggregatedDataPoint next() {
            currentIndex = internalIt.next();
            timestamp = startInterval.plus(currentIndex, aggregateInterval.getChronoUnit()).toInstant()
                    .toEpochMilli();
            return this;
        }

        @Override
        public int getCount() {
            return counts[currentIndex];
        }

        @Override
        public Stats getStats() {
            return new Stats() {
                @Override
                public int getCount() {
                    return counts[currentIndex];
                }

                @Override
                public double getSum(int measure) {
                    return Double.longBitsToDouble(aggsByMeasure[getMeasureIndex(measure)][currentIndex * 5]);
                }

                @Override
                public double getMinValue(int measure) {
                    return Double.longBitsToDouble(aggsByMeasure[getMeasureIndex(measure)][currentIndex * 5 + 1]);
                }

                @Override
                public double getMaxValue(int measure) {
                    return Double.longBitsToDouble(aggsByMeasure[getMeasureIndex(measure)][currentIndex * 5 + 3]);
                }

                @Override
                public double getAverageValue(int measure) {
                    return Double.longBitsToDouble(aggsByMeasure[getMeasureIndex(measure)][currentIndex * 5]) / counts[currentIndex];
                }

                @Override
                public long getMinTimestamp(int measure) {
                    return aggsByMeasure[getMeasureIndex(measure)][currentIndex * 5 + 2];
                }

                @Override
                public long getMaxTimestamp(int measure) {
                    return aggsByMeasure[getMeasureIndex(measure)][currentIndex * 5 + 4];
                }
            };
        }

        @Override
        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public double[] getValues() {
            throw new UnsupportedOperationException();
        }
    }

}
