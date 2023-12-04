package eu.more2020.visual.middleware.domain;

import java.util.List;

/**
 * A representation of aggregate statistics for multi-variate time series data points.
 */
public interface Stats {
    public int getCount();

    public double getSum();

    public double getMinValue();
    public long getMinTimestamp();

    public double getMaxValue();

    public long getMaxTimestamp();

    public double getFirstValue();

    public long getFirstTimestamp();

    public double getLastValue();

    public long getLastTimestamp();


    public double getAverageValue();

    default UnivariateDataPoint getMinDataPoint() {
        return new ImmutableUnivariateDataPoint(getMinTimestamp(), getMinValue());
    }

    default UnivariateDataPoint getMaxDataPoint() {
        return new ImmutableUnivariateDataPoint(getMaxTimestamp(), getMaxValue());
    }

    default UnivariateDataPoint getFirstDataPoint() {
        return new ImmutableUnivariateDataPoint(getFirstTimestamp(), getFirstValue());
    }

    default UnivariateDataPoint getLastDataPoint() {
        return new ImmutableUnivariateDataPoint(getLastTimestamp(), getLastValue());
    }



    default String getString(int measure) {
        return "{" +
                "measure=" + measure +
                ", count=" + getCount() +
                ", sum=" + getSum() +
                ", min=" + getMinValue() +
                ", minTimestamp=" + getMinTimestamp() +
                ", max=" + getMaxValue() +
                ", maxTimestamp=" + getMaxTimestamp() +
                ", first=" + getFirstValue() +
                ", firstTimestamp=" + getFirstTimestamp() +
                ", last=" + getLastValue() +
                ", lastTimestamp=" + getLastTimestamp() +
                ", average=" + getAverageValue() +
                '}';
    }

    default String toString(int measure) {
        return "{" +
                "measure=" + measure +
                ", count=" + getCount() +
                ", sum=" + getSum() +
                ", min=" + getMinValue() +
                ", minTimestamp=" + getMinTimestamp() +
                ", max=" + getMaxValue() +
                ", maxTimestamp=" + getMaxTimestamp() +
                ", first=" + getFirstValue() +
                ", firstTimestamp=" + getFirstTimestamp() +
                ", last=" + getLastValue() +
                ", lastTimestamp=" + getLastTimestamp() +
                ", average=" + getAverageValue() +
                '}';
    }

}
