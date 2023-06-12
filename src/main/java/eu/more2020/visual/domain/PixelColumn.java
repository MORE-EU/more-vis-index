package eu.more2020.visual.domain;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class PixelColumn implements TimeInterval {

    private static final Logger LOG = LoggerFactory.getLogger(PixelColumn.class);

    private final long from;
    private final long to;

    private final List<Integer> measures;

    private final ViewPort viewPort;


    private final StatsAggregator statsAggregator;

    private final RangeSet<Long> fullyContainedRangeSet = TreeRangeSet.create();


    // The left and right agg data points of this pixel column. These can be either partially-contained inside this pixel column and overlap, or fully-contained.
    private List<AggregatedDataPoint> left = new ArrayList<>();
    private List<AggregatedDataPoint> right = new ArrayList<>();

    // todo: remove this. just for debugging
    private List<AggregatedDataPoint> all = new ArrayList<>();


    public PixelColumn(long from, long to, List<Integer> measures, ViewPort viewPort) {
        this.from = from;
        this.to = to;
        this.measures = measures;
        statsAggregator = new StatsAggregator(measures);
        this.viewPort = viewPort;
    }

    public void addAggregatedDataPoint(AggregatedDataPoint dp) {

        all.add(ImmutableAggregatedDataPoint.fromAggregatedDataPoint(dp));

        if (dp.getFrom() <= from) {
            left.add(ImmutableAggregatedDataPoint.fromAggregatedDataPoint(dp));
        }

        if (dp.getTo() >= to) {
            right.add(ImmutableAggregatedDataPoint.fromAggregatedDataPoint(dp));
        }

        if (this.encloses(dp)) {
            fullyContainedRangeSet.add(TimeInterval.toGuavaRange(dp));
        }

        Stats stats = dp.getStats();

        // todo: here, in case we add data from time series span, we add the same min-max point twice. This is not a problem, but it's not optimal.
        for (int measure : measures) {
            if (this.contains(stats.getMinTimestamp(measure))) {
                statsAggregator.accept(dp.getStats().getMinDataPoint(measure), measure);
            }

            if (this.contains(stats.getMaxTimestamp(measure))) {
                statsAggregator.accept(dp.getStats().getMaxDataPoint(measure), measure);
            }

            if (this.contains(stats.getFirstTimestamp(measure))) {
                statsAggregator.accept(dp.getStats().getFirstDataPoint(measure), measure);
            }

            if (this.contains(stats.getLastTimestamp(measure))) {
                statsAggregator.accept(dp.getStats().getLastDataPoint(measure), measure);
            }
        }

    }

    /**
     * Returns the vertical pixel id of the given value for the specified measure
     * considering the current min and max values of the measure over the entire view port.
     *
     * @param m
     * @param value
     * @param windowStats
     * @return the vertical pixel id of the given value for the specified measure
     */
    private int getPixelId(int m, double value, StatsAggregator windowStats) {
        return (int) ((double) viewPort.getHeight() * (value - windowStats.getMinValue(m)) / (windowStats.getMaxValue(m) - windowStats.getMinValue(m)));
    }



    public int[] computeMaxInnerPixelError(StatsAggregator windowStats) {
        Range<Long> pixelColumnTimeRange = Range.closedOpen(from, to);

        int[] errors = new int[measures.size()];
        Arrays.fill(errors, -1);

        Set<Range<Long>> fullyContainedDisjointRanges = fullyContainedRangeSet.asRanges();

        if (fullyContainedDisjointRanges.size() > 1) {
            throw new IllegalArgumentException("There are gaps in the fully contained ranges of this pixel column.");
        } else if (fullyContainedDisjointRanges.size() == 0) {
            return errors;
        }

        Range<Long> fullyContainedRange = fullyContainedDisjointRanges.iterator().next();

        ImmutableRangeSet<Long> immutableFullyContainedRangeSet = ImmutableRangeSet.copyOf(fullyContainedRangeSet);

        // Compute difference between pixel column range and fullyContainedRangeSet
        ImmutableRangeSet<Long> differenceSet = ImmutableRangeSet.of(pixelColumnTimeRange).difference(immutableFullyContainedRangeSet);

        List<Range<Long>> differenceList = differenceSet.asRanges().stream()
                .collect(Collectors.toList());

        Range<Long> leftSubRange = null;
        Range<Long> rightSubRange = null;

        if (differenceList.size() == 2) {
            leftSubRange = differenceList.get(0);
            rightSubRange = differenceList.get(1);
        } else if (differenceList.size() == 1) {
            if (differenceList.get(0).lowerEndpoint() < fullyContainedRange.lowerEndpoint()) {
                leftSubRange = differenceList.get(0);
            } else {
                rightSubRange = differenceList.get(0);
            }
        }
        AggregatedDataPoint leftPartial = null;
        AggregatedDataPoint rightPartial = null;


        if (leftSubRange != null) {
            Range<Long> finalLeftSubRange = leftSubRange;
            leftPartial = left.stream().filter(aggregatedDataPoint -> aggregatedDataPoint.getTo() >= finalLeftSubRange.upperEndpoint())
                    .min(Comparator.comparingLong(aggregatedDataPoint -> aggregatedDataPoint.getTo() - aggregatedDataPoint.getFrom()))
                    .orElseThrow(() ->
                            new IllegalStateException("Could not determine the left partially contained group."));
        }
        if (rightSubRange != null) {
            Range<Long> finalRightSubRange = rightSubRange;
            rightPartial = right.stream().filter(aggregatedDataPoint -> aggregatedDataPoint.getFrom() <= finalRightSubRange.lowerEndpoint())
                    .min(Comparator.comparingLong(aggregatedDataPoint -> aggregatedDataPoint.getTo() - aggregatedDataPoint.getFrom()))
                    .orElseThrow(() -> {
                        LOG.error("Pixel Column: {}", this.getIntervalString());
                        LOG.error("Right: {}", right);
                        LOG.error("Right Sub Range: {}", finalRightSubRange);
                        all.stream().forEach(aggregatedDataPoint -> {
                            LOG.error("Agg Interval: {} Data Point: {}", aggregatedDataPoint.getTo() - aggregatedDataPoint.getFrom(), aggregatedDataPoint.getIntervalString());
                        });
                        return new IllegalStateException("Could not determine the right partially contained group.");
                    });
        }

        for (int i = 0; i < measures.size(); i++) {
            int measure = measures.get(i);
            RangeSet<Integer> pixelRangeSet = TreeRangeSet.create();
            if (leftPartial != null) {
                pixelRangeSet.add(Range.closed(getPixelId(measure, leftPartial.getStats().getMinValue(measure), windowStats), getPixelId(measure, leftPartial.getStats().getMinValue(measure), windowStats)));
            }
            if (rightPartial != null) {
                pixelRangeSet.add(Range.closed(getPixelId(measure, rightPartial.getStats().getMinValue(measure), windowStats), getPixelId(measure, rightPartial.getStats().getMinValue(measure), windowStats)));
            }
            pixelRangeSet.remove(Range.closed(getPixelId(measure, statsAggregator.getMinValue(measure), windowStats), getPixelId(measure, statsAggregator.getMaxValue(measure), windowStats)));

            errors[i] = pixelRangeSet.asRanges().stream()
                    .mapToInt(range -> range.upperEndpoint() - range.lowerEndpoint() + 1)
                    .sum();
        }
        LOG.debug("Inner Column Errors: {}", errors);
        return errors;
    }

    /**
     * Returns a closed range of pixel IDs that the line segment intersects within this pixel column.
     *
     * @param measure      The measure for which to calculate the pixel IDs.
     * @param t1           The first timestamp of the line segment.
     * @param v1           The value at the first timestamp of the line segment.
     * @param t2           The second timestamp of the line segment.
     * @param v2           The value at the second timestamp of the line segment.
     * @param windowStats  The stats for the entire view port.
     * @return A Range object representing the range of pixel IDs that the line segment intersects within the pixel column.
     */
    private Range<Integer> getPixelIdsForLineSegment(int measure, double t1, double v1, double t2, double v2, StatsAggregator windowStats) {
        // Calculate the slope of the line segment
        double slope = (v2 - v1) / (t2 - t1);

        // Calculate the y-intercept of the line segment
        double yIntercept = v1 - slope * t1;

        // Find the first and last timestamps of the line segment within the pixel column
        double tStart = Math.max(from, Math.min(t1, t2));
        double tEnd = Math.min(to, Math.max(t1, t2));

        // Calculate the values at the start and end timestamps
        double vStart = slope * tStart + yIntercept;
        double vEnd = slope * tEnd + yIntercept;

        // Convert the values to pixel ids
        int pixelIdStart = getPixelId(measure, vStart, windowStats);
        int pixelIdEnd = getPixelId(measure, vEnd, windowStats);

        // Create a range from the pixel ids and return it
        return Range.closed(Math.min(pixelIdStart, pixelIdEnd), Math.max(pixelIdStart, pixelIdEnd));
    }



    @Override
    public long getFrom() {
        return from;
    }

    @Override
    public long getTo() {
        return to;
    }


    public Stats getStats() {
        return statsAggregator;
    }


    @Override
    public String toString() {
        return "PixelColumn{ timeInterval: " + getIntervalString() + ", stats: " + statsAggregator + "}";
    }

}