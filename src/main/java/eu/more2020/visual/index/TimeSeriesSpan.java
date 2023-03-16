package eu.more2020.visual.index;

import eu.more2020.visual.domain.*;
import eu.more2020.visual.util.DateTimeUtil;
import org.apache.parquet.Log;

import javax.xml.crypto.Data;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
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

    /**
     * The aggregate values for every window interval and for every measure.
     */
    private final Map<Integer, long[]> aggsByMeasure;

    /**
     * The number of raw time series points behind every data point included in this time series span.
     * When the time series span corresponds to raw, non aggregated data, this number is 1.
     */
    private int[] counts;

    // The start time value of the span
    private long from;
    // The size of this span, corresponding to the number of aggregated window intervals represented by it
    private int size;

    private ZoneId zoneId;

    /**
     * The fixed window that raw data points are grouped by in this span.
     */
    private AggregateInterval aggregateInterval;

    public TimeSeriesSpan() {
        aggsByMeasure = new HashMap<>();
    }


    /**
     * @param dataPoints
     * @param aggregateInterval
     * @param zoneId
     */
    public void build(DataPoints dataPoints, AggregateInterval aggregateInterval, ZoneId zoneId) {
        this.aggregateInterval = aggregateInterval;
        this.zoneId = zoneId;
        size = DateTimeUtil.numberOfIntervals(dataPoints.getFrom(), dataPoints.getTo(), aggregateInterval, zoneId);
        List<Integer> measures = dataPoints.getMeasures();
        counts = new int[size];

        for (Integer measure : measures) {
            aggsByMeasure.put(measure, new long[size * 5]);
        }
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
        if (stats.getCount() == 0){
            return;
        }
        for (int j = 0; j < measures.size(); j++) {
            int m = measures.get(j);
            long[] data = aggsByMeasure.get(m);
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
        return aggsByMeasure.keySet().stream().sorted()
                .collect(Collectors.toList());
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

    public TimeRange getTimeRange(){
        return  new TimeRange(getFrom(), getTo());
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
                .addTo(ZonedDateTime.ofInstant(Instant.ofEpochMilli(from), zoneId), size * aggregateInterval.getInterval())
                .toInstant().toEpochMilli();
    }
    @Override
    public String getFromDate() {
        return Instant.ofEpochMilli(getFrom()).atZone(zoneId).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    @Override
    public String getToDate() {
        return Instant.ofEpochMilli(getTo()).atZone(zoneId).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    @Override
    public String toString() {
        return getFromDate() + " - " + getToDate() + " " + aggregateInterval;
    }

    public boolean hasData() { return getTo() == getFrom();}

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
                    return Double.longBitsToDouble(aggsByMeasure.get(measure)[currentIndex * 5]);
                }

                @Override
                public double getMinValue(int measure) {
                    return Double.longBitsToDouble(aggsByMeasure.get(measure)[currentIndex * 5 + 1]);
                }

                @Override
                public double getMaxValue(int measure) {
                    return Double.longBitsToDouble(aggsByMeasure.get(measure)[currentIndex * 5 + 3]);
                }

                @Override
                public double getAverageValue(int measure) {
                    return Double.longBitsToDouble(aggsByMeasure.get(measure)[currentIndex * 5]) / counts[currentIndex];
                }

                @Override
                public long getMinTimestamp(int measure) {
                    return aggsByMeasure.get(measure)[currentIndex * 5 + 2];
                }

                @Override
                public long getMaxTimestamp(int measure) {
                    return aggsByMeasure.get(measure)[currentIndex * 5 + 4];
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
