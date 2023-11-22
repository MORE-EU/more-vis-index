package eu.more2020.visual.middleware.domain;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import eu.more2020.visual.middleware.util.DateTimeUtil;
import org.apache.arrow.flatbuf.Int;
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

    private final StatsAggregator fullyContainedStatsAggregator;


    private AggregatedDataPoint leftPartial;
    private AggregatedDataPoint rightPartial;


    // The left and right agg data points of this pixel column. These can be either partially-contained inside this pixel column and overlap, or fully-contained.
    private List<AggregatedDataPoint> left = new ArrayList<>();
    private List<AggregatedDataPoint> right = new ArrayList<>();

    public PixelColumn(long from, long to, List<Integer> measures, ViewPort viewPort) {
        this.from = from;
        this.to = to;
        this.measures = measures;
        statsAggregator = new StatsAggregator(measures);
        fullyContainedStatsAggregator = new StatsAggregator(measures);
        this.viewPort = viewPort;
    }

    public void addDataPoint(DataPoint dp, List<Integer> measures){
        int i = 0;
        for (int measure : measures) {
            statsAggregator.accept(new UnivariateDataPoint(dp.getTimestamp(), dp.getValues()[i]), measure);
            fullyContainedStatsAggregator.accept(new UnivariateDataPoint(dp.getTimestamp(), dp.getValues()[i]), measure);
            i ++;
        }
        fullyContainedRangeSet.add(TimeInterval.toGuavaRange(new TimeRange(from, dp.getTimestamp())));
    }

    public void addAggregatedDataPoint(AggregatedDataPoint dp) {
        if (dp.getFrom() <= from) {
            left.add(ImmutableAggregatedDataPoint.fromAggregatedDataPoint(dp));
        }

        if (dp.getTo() >= to) {
            right.add(ImmutableAggregatedDataPoint.fromAggregatedDataPoint(dp));
        }

        Stats stats = dp.getStats();
        if (this.encloses(dp)) {
            fullyContainedRangeSet.add(TimeInterval.toGuavaRange(dp));
            if (stats.getCount() > 0)
                for (int measure : measures) {
                    if(!dp.getStats().getMeasures().contains(measure)) continue; // check if aggregate datapoint has this measure
                    fullyContainedStatsAggregator.accept(stats.getMinDataPoint(measure), measure);
                    fullyContainedStatsAggregator.accept(stats.getMaxDataPoint(measure), measure);
                }
        }

        // todo: here, in case we add data from time series span, we add the same min-max point twice. This is not a problem, but it's not optimal.
        if (stats.getCount() > 0)
            for (int measure : measures) {
                if(!dp.getStats().getMeasures().contains(measure)) continue; // check if aggregate datapoint has this measure
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
     * @param viewPortStats
     * @return the vertical pixel id of the given value for the specified measure
     */
    private int getPixelId(int m, double value, Stats viewPortStats) {
        return (int) ((double) viewPort.getHeight() * (value - viewPortStats.getMinValue(m)) / (viewPortStats.getMaxValue(m) - viewPortStats.getMinValue(m)));
    }

    // Takes two aggregatedDataPoints with different measures and return 1 with the measures combined
    private AggregatedDataPoint combineAggregateDataPoints(AggregatedDataPoint aggregatedDataPoint1, AggregatedDataPoint aggregatedDataPoint2) {
        List<Integer> measures = new ArrayList<>(aggregatedDataPoint1.getStats().getMeasures());
        measures.addAll(aggregatedDataPoint2.getStats().getMeasures());
        NonTimestampedStatsAggregator statsAggregator = new NonTimestampedStatsAggregator(measures);
        statsAggregator.combine(aggregatedDataPoint1.getStats());
        statsAggregator.combine(aggregatedDataPoint2.getStats());
        return new ImmutableAggregatedDataPoint(Math.min(aggregatedDataPoint1.getFrom(), aggregatedDataPoint2.getFrom()),
                Math.max(aggregatedDataPoint1.getTo(), aggregatedDataPoint2.getTo()), statsAggregator);
    }

    private void determinePartialContained() {
        Range<Long> pixelColumnTimeRange = Range.closedOpen(from, to);
        Range<Long> fullyContainedRange = fullyContainedRangeSet.span();

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

        if (leftSubRange != null) {
            Range<Long> finalLeftSubRange = leftSubRange;
            if(left.size() == 0) leftPartial = null;
            else {
                leftPartial = left.stream().filter(aggregatedDataPoint -> aggregatedDataPoint.getTo() >= finalLeftSubRange.upperEndpoint())
                            .reduce(this::combineAggregateDataPoints) // Using the combine method
//                          .min(Comparator.comparingLong(aggregatedDataPoint -> aggregatedDataPoint.getTo() - aggregatedDataPoint.getFrom()))
                            .orElseGet(() ->  null);
//                          .orElseThrow(() ->
//                                new IllegalStateException("Could not determine the left partially contained group " +
//                                        DateTimeUtil.format(getFrom()) + " - " + DateTimeUtil.format(getTo())));
            }
        } else {
            leftPartial = null;
        }
        if (rightSubRange != null) {
            Range<Long> finalRightSubRange = rightSubRange;
            if(right.size() == 0) rightPartial = null;
            else
                rightPartial = right.stream().filter(aggregatedDataPoint -> aggregatedDataPoint.getFrom() <= finalRightSubRange.lowerEndpoint())
                        .reduce(this::combineAggregateDataPoints) // Using the combine method
//                      .min(Comparator.comparingLong(aggregatedDataPoint -> aggregatedDataPoint.getTo() - aggregatedDataPoint.getFrom()))
                        .orElseGet(() ->  null);
//                      .orElseThrow(() ->
//                            new IllegalStateException("Could not determine the right partially contained group " +
//                                    DateTimeUtil.format(getFrom()) + " - "  + DateTimeUtil.format(getTo())));
        } else {
            rightPartial = null;
        }
    }

    /**
     * Computes the maximum inner pixel range for each measure. For this we consider both the fully contained and the partially contained groups.
     * @param viewPortStats
     * @return the maximum inner column pixel range for each measure or null if there are gaps in the fully contained ranges or no fully contained ranges at all.
     */

    public List<Range<Integer>> computeMaxInnerPixelRange(Stats viewPortStats) {
        Set<Range<Long>> fullyContainedDisjointRanges = fullyContainedRangeSet.asRanges();
        if (fullyContainedDisjointRanges.size() > 1) {
            LOG.info("There are gaps in the fully contained ranges of this pixel column.");
            return null;
        } else if (fullyContainedDisjointRanges.size() == 0) {
            LOG.debug("There is no fully contained range in this pixel column.");
            return null;
        }
        determinePartialContained();
        return measures.stream().map(m -> {
            // TODO: Currently checking if measure has been initialized by checking the minTimestamp.
            // While this is not wrong it is not so clear. We have to have different counts for each measure.
            if(statsAggregator.getCount() > 0 && statsAggregator.getMinTimestamp(m) != -1) {
                int minPixelId = getPixelId(m, statsAggregator.getMinValue(m), viewPortStats);
                int maxPixelId = getPixelId(m, statsAggregator.getMaxValue(m), viewPortStats);
                if (leftPartial != null && leftPartial.getCount() > 0 && leftPartial.getStats().getMeasures().contains(m)) {
                    minPixelId = Math.min(minPixelId, getPixelId(m, leftPartial.getStats().getMinValue(m), viewPortStats));
                    maxPixelId = Math.max(maxPixelId, getPixelId(m, leftPartial.getStats().getMaxValue(m), viewPortStats));
                }
                else return Range.closed(0, 0);
                if (rightPartial != null && rightPartial.getCount() > 0 && rightPartial.getStats().getMeasures().contains(m))  {
                    minPixelId = Math.min(minPixelId, getPixelId(m, rightPartial.getStats().getMinValue(m), viewPortStats));
                    maxPixelId = Math.max(maxPixelId, getPixelId(m, rightPartial.getStats().getMaxValue(m), viewPortStats));
                }
                else return Range.closed(0, 0);
                return Range.closed(minPixelId, maxPixelId);
            } else return null;
        }).collect(Collectors.toList());
    }


    /**
     * Returns a closed range of pixel IDs that the line segment intersects within this pixel column.
     *
     * @param measure       The measure for which to calculate the pixel IDs.
     * @param t1            The first timestamp of the line segment.
     * @param v1            The value at the first timestamp of the line segment.
     * @param t2            The second timestamp of the line segment.
     * @param v2            The value at the second timestamp of the line segment.
     * @param viewPortStats The stats for the entire view port.
     * @return A Range object representing the range of pixel IDs that the line segment intersects within the pixel column.
     */
    public Range<Integer> getPixelIdsForLineSegment(int measure, double t1, double v1, double t2, double v2, Stats viewPortStats) {
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
        int pixelIdStart = getPixelId(measure, vStart, viewPortStats);
        int pixelIdEnd = getPixelId(measure, vEnd, viewPortStats);

        // Create a range from the pixel ids and return it
        return Range.closed(Math.min(pixelIdStart, pixelIdEnd), Math.max(pixelIdStart, pixelIdEnd));
    }

    /**
     * Returns the range of inner-column pixel IDs that can be correctly determined for this pixel column for the give measure.
     * This range is determined by the min and max values over the fully contained groups in this pixel column.
     *
     * @param measure
     * @param viewPortStats The stats for the entire view port.
     * @return A Range object representing the range of inner-column pixel IDs
     */
    public Range<Integer> getActualInnerColumnPixelRange(int measure, Stats viewPortStats) {
        // TODO: Again here we check for missing data with the minTimestamp which can be improved.
        if(fullyContainedStatsAggregator.getCount() <= 0 ||
                fullyContainedStatsAggregator.getMinTimestamp(measure) == -1) return Range.closed(0, 0); // If not initialized or empty
//        LOG.info("Fully contained stats: {}", fullyContainedStatsAggregator);
        return Range.closed(getPixelId(measure, fullyContainedStatsAggregator.getMinValue(measure), viewPortStats),
                getPixelId(measure, fullyContainedStatsAggregator.getMaxValue(measure), viewPortStats));
    }


    @Override
    public long getFrom() {
        return from;
    }

    @Override
    public long getTo() {
        return to;
    }

    public TimeInterval getRange(){
        return new TimeRange(from, to);
    }

    public Stats getStats() {
        return statsAggregator;
    }

    public AggregatedDataPoint getLeftPartial() {
        return leftPartial;
    }

    public AggregatedDataPoint getRightPartial() {
        return rightPartial;
    }

    @Override
    public String toString() {
        return "PixelColumn{ timeInterval: " + getIntervalString() + ", stats: " + statsAggregator + "}";
    }

}