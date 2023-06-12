package eu.more2020.visual.index;

import eu.more2020.visual.domain.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesSpan.class);

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

    // The end time value of the span
    // Keep in mind that the end time is not included in the span,
    // and also that the end time is not necessarily aligned with the aggregation interval
    private long to;

    // The size of this span, corresponding to the number of aggregated window intervals represented by it
    private int size;

    /**
     * The fixed window that raw data points are grouped by in this span.
     * Note that due to rounding, the last group of the span may cover a larger time interval
     */
    private long aggregateInterval;

    private void initialize(long from, long to, int size, List<Integer> measures) {
        this.size = size;
        this.from = from;
        this.to = to;
        this.aggregateInterval = (to - from) / size;
        LOG.debug("Initializing time series span [{},{}) with size {} and aggregate interval {}", from, to, size, aggregateInterval);
        this.measures = measures.stream().mapToInt(Integer::intValue).toArray();
        this.counts = new int[size];
        this.aggsByMeasure = new long[this.measures.length][size * 5];
    }


    public TimeSeriesSpan(long from, long to, List<Integer> measures, int size) {
        initialize(from, to, size, measures);
    }

    protected void addAggregatedDataPoint(int i, AggregatedDataPoint aggregatedDataPoint) {
        Stats stats = aggregatedDataPoint.getStats();
        counts[i] = stats.getCount();
        if (stats.getCount() == 0) {
            return;
        }
        for (int j = 0; j < measures.length; j++) {
            int m = measures[j];
            long[] data = aggsByMeasure[j];
            long minTimestamp = stats.getMinTimestamp(m);
            long maxTimestamp = stats.getMaxTimestamp(m);
            double minValue = stats.getMinValue(m);
            double maxValue = stats.getMaxValue(m);

            data[5 * i] = Double.doubleToRawLongBits(stats.getSum(m));
            data[5 * i + 1] = Double.doubleToRawLongBits(minValue);
            data[5 * i + 2] = minTimestamp;
            data[5 * i + 3] = Double.doubleToRawLongBits(maxValue);
            // not sure if this helps. we do it to keep the last timestamp in case of same values in the interval
            if (maxValue == stats.getLastValue(m)){
                data[5 * i + 4] = stats.getLastTimestamp(m);
            } else {
                data[5 * i + 4] = maxTimestamp;
            }
        }
    }

    /**
     * Finds the index in the span in which the given timestamp should be.
     * If the timestamp is before the start of the span, the first index is returned.
     * If the timestamp is after the end of the span, the last index is returned.
     *
     * @param timestamp A timestamp in milliseconds since epoch.
     * @return A positive index.
     */
    private int getIndex(final long timestamp) {
        int index = (int) ((double) size * (timestamp - from) / (to - from));
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

    public long getAggregateInterval() {
        return aggregateInterval;
    }

    public int[] getCounts() {
        return counts;
    }


    /**
     * Returnts an iterator over the aggregated data points in this span that fall within the given time range.
     *
     * @param queryStartTimestamp The start timestamp of the query.
     * @param queryEndTimestamp   The end timestamp of the query (not included).
     * @return The iterator.
     */
    public Iterator<AggregatedDataPoint> iterator(long queryStartTimestamp, long queryEndTimestamp) {
        return new TimeSeriesSpanIterator(queryStartTimestamp, queryEndTimestamp);
    }

/*
    public Iterator<UnivariateDataPoint> getMinMaxIterator(long queryStartTimestamp, long queryEndTimestamp, int measure) {
        return new MinMaxIterator(queryStartTimestamp, queryEndTimestamp, measure);
    }
*/


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
        return to;
    }

    @Override
    public String toString() {
        return "{[" + getFromDate() + "(" + getFrom() + ")" +
                ", " + getToDate() + "(" + getTo() + ")" +
                "), size=" + size + ", aggregateInterval=" + aggregateInterval + "}";
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
        final int ARRAY_OVERHEAD = 20;
        // Memory usage of int in a 64-bit JVM
        final int INT_SIZE = 4;
        // Memory usage of long in a 64-bit JVM
        final int LONG_SIZE = 8;
        // Memory usage of a reference in a 64-bit JVM with a heap size less than 32 GB
        final int REF_SIZE = 4;


        long measuresMemory = REF_SIZE + ARRAY_OVERHEAD + (measures.length * INT_SIZE);

        long aggsByMeasureMemory = REF_SIZE + ARRAY_OVERHEAD + aggsByMeasure.length * (REF_SIZE + ARRAY_OVERHEAD + (size * 5 * LONG_SIZE));

        long countsMemory = REF_SIZE + ARRAY_OVERHEAD + (counts.length * INT_SIZE);

        long aggregateIntervalMemory = 2 * REF_SIZE + OBJECT_OVERHEAD + LONG_SIZE;

        long deepMemorySize = REF_SIZE + OBJECT_OVERHEAD + measuresMemory +
                aggsByMeasureMemory + countsMemory + LONG_SIZE + INT_SIZE + aggregateIntervalMemory;


        return deepMemorySize;
    }


/*    public TimeSeriesSpan rollup(AggregateInterval newAggregateInterval) {
        // Validate that the new aggregate interval is larger than the current one
        if (newAggregateInterval.toDuration().compareTo(this.aggregateInterval.toDuration()) <= 0) {
            throw new IllegalArgumentException("The new aggregate interval must be larger than the current one.");
        }
        return TimeSeriesSpanFactory.createFromRaw(this, newAggregateInterval);
    }*/

    private class TimeSeriesSpanIterator implements Iterator<AggregatedDataPoint>, AggregatedDataPoint {

        private Iterator<Integer> internalIt;

        private long timestamp;

        private int currentIndex = -1;

        public TimeSeriesSpanIterator(long queryStartTimestamp, long queryEndTimestamp) {
            internalIt = IntStream.range(getIndex(queryStartTimestamp), queryEndTimestamp >= 0 ? getIndex(queryEndTimestamp - 1) + 1 : size)
                    .iterator();
        }

        @Override
        public boolean hasNext() {
            return internalIt.hasNext();
        }

        @Override
        public AggregatedDataPoint next() {
            currentIndex = internalIt.next();
            timestamp = from + currentIndex * aggregateInterval;
            return this;
        }

        @Override
        public int getCount() {
            return counts[currentIndex];
        }

        @Override
        public Stats getStats() {
            return new Stats() {

                private int index = currentIndex;

                @Override
                public int getCount() {
                    return counts[index];
                }

                @Override
                public double getSum(int measure) {
                    return Double.longBitsToDouble(aggsByMeasure[getMeasureIndex(measure)][index * 5]);
                }

                @Override
                public double getMinValue(int measure) {
                    return Double.longBitsToDouble(aggsByMeasure[getMeasureIndex(measure)][index * 5 + 1]);
                }

                @Override
                public double getMaxValue(int measure) {
                    return Double.longBitsToDouble(aggsByMeasure[getMeasureIndex(measure)][index * 5 + 3]);
                }

                @Override
                public double getAverageValue(int measure) {
                    return Double.longBitsToDouble(aggsByMeasure[getMeasureIndex(measure)][index * 5]) / counts[index];
                }

                @Override
                public long getMinTimestamp(int measure) {
                    return aggsByMeasure[getMeasureIndex(measure)][index * 5 + 2];
                }

                @Override
                public long getMaxTimestamp(int measure) {
                    return aggsByMeasure[getMeasureIndex(measure)][index * 5 + 4];
                }

                @Override
                public double getFirstValue(int measure) {
                    return getMinTimestamp(measure) < getMaxTimestamp(measure) ?
                            getMinValue(measure) : getMaxValue(measure);
                }

                @Override
                public long getFirstTimestamp(int measure) {
                    return getMinTimestamp(measure) < getMaxTimestamp(measure) ?
                            getMinTimestamp(measure) : getMaxTimestamp(measure);
                }

                @Override
                public double getLastValue(int measure) {
                    return getMinTimestamp(measure) > getMaxTimestamp(measure) ?
                            getMinValue(measure) : getMaxValue(measure);
                }

                @Override
                public long getLastTimestamp(int measure) {
                    return getMinTimestamp(measure) > getMaxTimestamp(measure) ?
                            getMinTimestamp(measure) : getMaxTimestamp(measure);
                }
            };
        }

        @Override
        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public long getFrom() {
            return timestamp;
        }

        @Override
        public long getTo() {
            if (currentIndex == size - 1) {
                return to;
            } else {
                return from + (currentIndex + 1) * aggregateInterval;
            }
        }

        @Override
        public double[] getValues() {
            throw new UnsupportedOperationException();
        }
    }

    /*private class MinMaxIterator implements Iterator<UnivariateDataPoint> {
        private int currentIdx;
        private int maxIdx;
        private int measureIndex;
        private UnivariateDataPoint nextMin, nextMax;

        private long queryStartTimestamp;

        private long queryEndTimestamp;

        public MinMaxIterator(long queryStartTimestamp, long queryEndTimestamp, int measure) {
            this.queryStartTimestamp = queryStartTimestamp;
            this.queryEndTimestamp = queryEndTimestamp;
            this.currentIdx = getIndex(queryStartTimestamp);
            this.maxIdx = queryEndTimestamp >= 0 ? getIndex(queryEndTimestamp) : size - 1;
            this.measureIndex = getMeasureIndex(measure);
            if (this.measureIndex < 0) {
                throw new IllegalArgumentException("Measure not found in TimeSeriesSpan");
            }
            advance();
        }

        private void advance() {
            while ((nextMin == null && nextMax == null) && currentIdx <= maxIdx) {
                long[] data = aggsByMeasure[measureIndex];
                if (counts[currentIdx] == 0) {
                    currentIdx++;
                    continue;
                }

                long minTimestamp = data[currentIdx * 5 + 2];
                double minValue = Double.longBitsToDouble(data[currentIdx * 5 + 1]);
                if (minTimestamp >= queryStartTimestamp && minTimestamp < queryEndTimestamp) {
                    nextMin = new UnivariateDataPoint(minTimestamp, minValue);
                }

                long maxTimestamp = data[currentIdx * 5 + 4];
                double maxValue = Double.longBitsToDouble(data[currentIdx * 5 + 3]);
                if (maxTimestamp >= queryStartTimestamp && maxTimestamp < queryEndTimestamp) {
                    nextMax = new UnivariateDataPoint(maxTimestamp, maxValue);
                }
                currentIdx++;
            }
        }

        @Override
        public boolean hasNext() {
            return nextMin != null || nextMax != null;
        }

        public UnivariateDataPoint next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            UnivariateDataPoint result;
            if (nextMin != null && (nextMax == null || nextMin.getTimestamp() <= nextMax.getTimestamp())) {
                result = nextMin;
                nextMin = null;
            } else {
                result = nextMax;
                nextMax = null;
            }
            advance();
            return result;
        }
    }*/

}
