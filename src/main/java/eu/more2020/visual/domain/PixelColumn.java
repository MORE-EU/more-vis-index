package eu.more2020.visual.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class PixelColumn implements TimeInterval {

    private static final Logger LOG = LoggerFactory.getLogger(PixelColumn.class);

    private final long from;
    private final long to;

    List<Integer> measures;

    private final StatsAggregator statsAggregator;


    // The left and right agg data points of this pixel column. These can be either partially-contained inside this pixel column and overlap, or fully-contained.
    private List<AggregatedDataPoint> left = new ArrayList<>();
    private List<AggregatedDataPoint> right = new ArrayList<>();

    public PixelColumn(long from, long to, List<Integer> measures) {
        this.from = from;
        this.to = to;
        this.measures = measures;
        statsAggregator = new StatsAggregator(measures);
    }

    public void addAggregatedDataPoint(AggregatedDataPoint dp) {

        if (dp.getFrom() <= from) {
            left.add(ImmutableAggregatedDataPoint.fromAggregatedDataPoint(dp));
        }

        if (dp.getTo() >= to) {
            right.add(ImmutableAggregatedDataPoint.fromAggregatedDataPoint(dp));
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

    public List<AggregatedDataPoint> getLeft() {
        return left;
    }

    public List<AggregatedDataPoint> getRight() {
        return right;
    }

    @Override
    public String toString() {
        return "PixelColumn{ timeInterval: " + getIntervalString() + ", stats: " + statsAggregator + "}";
    }

}