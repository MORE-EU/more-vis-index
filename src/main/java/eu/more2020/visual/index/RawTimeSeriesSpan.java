package eu.more2020.visual.index;

import eu.more2020.visual.domain.DataPoint;
import eu.more2020.visual.domain.DataPoints;
import eu.more2020.visual.domain.TimeInterval;
import eu.more2020.visual.domain.TimeRange;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * A {@link DataPoints} implementation that stores  a series of consecutive
 * raw data points.
 */
public class RawTimeSeriesSpan implements DataPoints, TimeInterval {

    int[] measures;

    /**
     * The raw datapoint values. The values of a datapoint for each measure are stored consecutively in the array.
     */
    private double[] values;

    /**
     * The timestamps of the raw datapoints.
     */
    private long[] timestamps;

    /**
     * @param dataPoints
     */
    public void build(DataPoints dataPoints) {
        measures = dataPoints.getMeasures().stream().mapToInt(Integer::intValue).toArray();

        ArrayList<Double> valuesList = new ArrayList<>();
        ArrayList<Long> timestampsList = new ArrayList<>();
        for (DataPoint dataPoint : dataPoints) {
            timestampsList.add(dataPoint.getTimestamp());
            for (double value : dataPoint.getValues()) {
                valuesList.add(value);
            }
        }
        values = valuesList.stream().mapToDouble(Double::doubleValue).toArray();
        timestamps = timestampsList.stream().mapToLong(Long::longValue).toArray();
    }


    /**
     * Finds the index in the span in which the given timestamp should be.
     *
     * @param timestamp A timestamp in milliseconds since epoch.
     * @return A positive index.
     */
    private int getIndex(final long timestamp) {
        int index = Arrays.binarySearch(timestamps, timestamp);
        if (index < 0) {
            // If not exact match, convert negative index to insertion point
            index = -(index + 1);
        }
        return index;
    }

    public List<Integer> getMeasures() {
        return Arrays.stream(measures).boxed().collect(Collectors.toList());
    }


    public TimeRange getTimeRange() {
        return new TimeRange(getFrom(), getTo());
    }

    public Iterator<DataPoint> iterator(long queryStartTimestamp, long queryEndTimestamp) {
        return new RawTimeSeriesSpanIterator(queryStartTimestamp, queryEndTimestamp);
    }


    @Override
    public Iterator<DataPoint> iterator() {
        // Use the first and last timestamps as the range for the iterator
        return new RawTimeSeriesSpanIterator(timestamps[0], timestamps[timestamps.length - 1] + 1);
    }


    @Override
    public long getFrom() {
        return timestamps[0];
    }

    @Override
    public long getTo() {
        return timestamps[timestamps.length - 1];
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
        return getFromDate() + " - " + getToDate();
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
        // Memory usage of double in a 64-bit JVM
        final int DOUBLE_SIZE = 8;
        // Memory usage of a reference in a 64-bit JVM with a heap size less than 32 GB
        final int REF_SIZE = 4;


        long measuresMemory = REF_SIZE + ARRAY_OVERHEAD + (measures.length * INT_SIZE);

        long valuesByMeasureMemory = REF_SIZE + ARRAY_OVERHEAD + (values.length * DOUBLE_SIZE);

        long timestampsMemory = REF_SIZE + ARRAY_OVERHEAD + (timestamps.length * LONG_SIZE);

        long deepMemorySize = REF_SIZE + OBJECT_OVERHEAD + measuresMemory +
                valuesByMeasureMemory + timestampsMemory;

        return deepMemorySize;
    }

    private class RawTimeSeriesSpanIterator implements Iterator<DataPoint> {
        private int startIndex;
        private int endIndex;
        private int currentIndex;

        public RawTimeSeriesSpanIterator(long queryStartTimestamp, long queryEndTimestamp) {
            startIndex = getIndex(queryStartTimestamp);
            endIndex = getIndex(queryEndTimestamp);
            currentIndex = startIndex;
        }

        @Override
        public boolean hasNext() {
            return currentIndex < endIndex;
        }

        @Override
        public DataPoint next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            final long timestamp = timestamps[currentIndex];
            final double[] values = Arrays.copyOfRange(RawTimeSeriesSpan.this.values,
                    currentIndex * measures.length, (currentIndex + 1) * measures.length);
            currentIndex++;

            return new DataPoint() {
                @Override
                public long getTimestamp() {
                    return timestamp;
                }

                @Override
                public double[] getValues() {
                    return values;
                }
            };
        }
    }
}
