package eu.more2020.visual.index;

import eu.more2020.visual.domain.*;
import eu.more2020.visual.util.DateTimeUtil;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A {@link DataPoints} implementation that aggregates a series of consecutive
 * raw data points based on the specified aggregation interval.
 * For each aggregation interval included and for each measure we store 3 doubles,
 * i.e. the sum, min and max aggregate values, as well as the corresponding
 * non-missing value counts.
 */
public class TimeSeriesSpan implements DataPoints, TimeInterval {

    /**
     * The aggregate values for every window interval and for every measure.
     * For each measure we store the corresponding aggregate values in an array, with 3 doubles needed for each group by window.
     * Specifically, we store the "sum", "min" and "max" values.
     */
    private final Map<Integer, long[]> aggsByMeasure;

    /**
     * The number of raw time series points behind every data point included in this time series span.
     * When the time series span corresponds to raw, non aggregated data, this number is 1.
     */
    private final int[] counts;

    // The start time value of the span
    private long from;
    // The size of this span, corresponding to the number of aggregated window intervals represented by it
    private int size;
    /**
     * The fixed window that raw data points are grouped by in this span.
     * It requires the {@link #unit} to fully define the group-by window.
     */
    private int interval;
    /**
     * the unit of the interval argument
     */
    private ChronoUnit unit;

    public TimeSeriesSpan() {
        aggsByMeasure = new HashMap<>();
        counts = new int[size];
    }


    /**
     * @param dataPoints
     * @param interval
     * @param unit
     * @param zoneId
     */
    public void build(DataPoints dataPoints, int interval, ChronoUnit unit, ZoneId zoneId) {
        this.unit = unit;
        this.interval = interval;
        size = DateTimeUtil.numberOfIntervals(dataPoints.getFrom(), dataPoints.getTo(), interval, unit, zoneId);
        List<Integer> measures = dataPoints.getMeasures();

        for (Integer measure : measures) {
            aggsByMeasure.put(measure, new long[size * 3]);
        }
        TimeAggregator timeAggregator = new TimeAggregator(dataPoints, interval, unit);

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
        for (int j = 0; j < measures.size(); j++) {
            int m = measures.get(j);
            long[] data = aggsByMeasure.get(m);
            data[3 * i] = Double.doubleToRawLongBits(stats.getSums()[j]);
            data[3 * i + 1] = Double.doubleToRawLongBits(stats.getMinValues()[j]);
            data[3 * i + 2] = stats.getMinTimestamps()[j];
            data[3 * i + 3] = Double.doubleToRawLongBits(stats.getMaxValues()[j]);
            data[3 * i + 4] = stats.getMaxTimestamps()[j];
        }
    }

    /**
     * Finds the index in the span in which the given timestamp should be.
     *
     * @param timestamp A timestamp in milliseconds since epoch.
     * @return A positive index.
     */
    private int getIndex(final long timestamp) {
        int index = DateTimeUtil.numberOfIntervals(from, timestamp, interval, unit, ZoneId.of("UTC")) - 1;
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

    public int getInterval() {
        return interval;
    }

    public ChronoUnit getUnit() {
        return unit;
    }

    public Iterator<DataPoint> iterator(long queryStartTimestamp, long queryEndTimestamp, Aggregator aggregator) {
        Iterator<Integer> internalIt = IntStream.range(getIndex(queryStartTimestamp), queryEndTimestamp >= 0 ? getIndex(queryEndTimestamp) + 1 : size)
                .iterator();
        ZonedDateTime startInterval = DateTimeUtil.getIntervalStart(from, interval, unit, ZoneId.of("UTC"));
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return internalIt.hasNext();
            }

            @Override
            public DataPoint next() {
                int index = internalIt.next();
                long timestamp = startInterval.plus(index, unit).toInstant().toEpochMilli();
                List<Integer> measures = getMeasures();
                double[] values = new double[measures.size()];

                for (int i = 0; i < measures.size(); i++) {
                    int measure = measures.get(i);
                    switch (aggregator) {
                        case SUM:
                            values[i] = Double.longBitsToDouble(aggsByMeasure.get(measure)[index * 3]);
                            break;
                        case MIN:
                            values[i] = Double.longBitsToDouble(aggsByMeasure.get(measure)[index * 3 + 1]);
                            break;
                        case MAX:
                            values[i] = Double.longBitsToDouble(aggsByMeasure.get(measure)[index * 3 + 3]);
                            break;
                        case AVG:
                            values[i] = Double.longBitsToDouble(aggsByMeasure.get(measure)[index * 3]) / counts[index];
                            break;
                    }
                }
                return new ImmutableDataPoint(timestamp, values);
            }


        };
    }

    @Override
    public Iterator iterator() {
        return iterator(from, -1, Aggregator.AVG);
    }


    @Override
    public long getFrom() {
        return from;
    }

    @Override
    public long getTo() {
        return from + size * interval;
    }

}
