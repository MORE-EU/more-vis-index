package eu.more2020.visual.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PixelColumn implements TimeInterval {

    private static final Logger LOG = LoggerFactory.getLogger(PixelColumn.class);

    private final long from;
    private final long to;

    List<Integer> measures;

    private final Map<Integer, UnivariateDataPoint> minPoints;
    private final Map<Integer, UnivariateDataPoint> maxPoints;
    private final Map<Integer, UnivariateDataPoint> firstPoints;
    private final Map<Integer, UnivariateDataPoint> lastPoints;


    // The left and right agg data points of this pixel column. These can be either partially-contained inside this pixel column and overlap, or fully-contained.
    private List<AggregatedDataPoint> left = new ArrayList<>();
    private List<AggregatedDataPoint> right = new ArrayList<>();

    public PixelColumn(long from, long to, List<Integer> measures) {
        this.from = from;
        this.to = to;
        minPoints = new HashMap<>();
        maxPoints = new HashMap<>();
        firstPoints = new HashMap<>();
        lastPoints = new HashMap<>();
        this.measures = measures;
    }

    public void addAggregatedDataPoint(AggregatedDataPoint dp) {

        if (dp.getFrom() <= from) {
            left.add(ImmutableAggregatedDataPoint.fromAggregatedDataPoint(dp));
        }

        if (dp.getTo() >= to) {
            right.add(ImmutableAggregatedDataPoint.fromAggregatedDataPoint(dp));
        }

        Stats stats = dp.getStats();


        if (stats.getCount() != 0) {
            for (int measure : measures) {
                if (this.contains(stats.getMinTimestamp(measure))) {
                    UnivariateDataPoint minPoint = new UnivariateDataPoint(stats.getMinTimestamp(measure), stats.getMinValue(measure));
//                    LOG.debug("Adding MIN data point with timestamp: " + minPoint.getTimestamp() + " and value: " + minPoint.getValue());
                    addUnivariateDataPoint(minPoint, measure);
                }
                if (this.contains(stats.getMaxTimestamp(measure))) {
                    UnivariateDataPoint maxPoint = new UnivariateDataPoint(stats.getMaxTimestamp(measure), stats.getMaxValue(measure));
//                    LOG.debug("Adding MAX data point with timestamp: " + maxPoint.getTimestamp() + " and value: " + maxPoint.getValue());
                    this.addUnivariateDataPoint(maxPoint, measure);
                }
            }
        }
    }


    public void addUnivariateDataPoint(UnivariateDataPoint dp, int measure) {
        UnivariateDataPoint minPoint = minPoints.get(measure);
        UnivariateDataPoint maxPoint = maxPoints.get(measure);
        UnivariateDataPoint firstPoint = firstPoints.get(measure);
        UnivariateDataPoint lastPoint = lastPoints.get(measure);

        if (minPoint == null) {
            minPoints.put(measure, dp);
            maxPoints.put(measure, dp);
            firstPoints.put(measure, dp);
            lastPoints.put(measure, dp);
            return;
        }

        if (dp.getValue() < minPoint.getValue()) {
            minPoints.put(measure, dp);
        } else if (dp.getValue() > maxPoint.getValue()) {
            maxPoints.put(measure, dp);
        }

        if (dp.getTimestamp() < firstPoint.getTimestamp()) {
            firstPoints.put(measure, dp);
        } else if (dp.getTimestamp() > lastPoint.getTimestamp()) {
            lastPoints.put(measure, dp);
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


    public Map<Integer, UnivariateDataPoint> getMinPoints() {
        return minPoints;
    }

    public Map<Integer, UnivariateDataPoint> getMaxPoints() {
        return maxPoints;
    }

    public Map<Integer, UnivariateDataPoint> getFirstPoints() {
        return firstPoints;
    }

    public Map<Integer, UnivariateDataPoint> getLastPoints() {
        return lastPoints;
    }

    public List<AggregatedDataPoint> getLeft() {
        return left;
    }

    public List<AggregatedDataPoint> getRight() {
        return right;
    }

    @Override
    public String toString() {
        return "PixelColumn{ timeInterval: " + getIntervalString() + "}";
    }

}