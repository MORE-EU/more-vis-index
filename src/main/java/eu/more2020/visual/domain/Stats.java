package eu.more2020.visual.domain;

/**
 * A representation of aggregate statistics for multi-variate time series data points.
 */
public interface Stats {

    public int getCount();

    public double getSum(int measure);

    public double getMinValue(int measure);

    public long getMinTimestamp(int measure);

    public double getMaxValue(int measure);

    public long getMaxTimestamp(int measure);

    public double getFirstValue(int measure);

    public long getFirstTimestamp(int measure);

    public double getLastValue(int measure);

    public long getLastTimestamp(int measure);


    public double getAverageValue(int measure);

    default UnivariateDataPoint getMinDataPoint(int measure) {
        return new UnivariateDataPoint(getMinTimestamp(measure), getMinValue(measure));
    }

    default UnivariateDataPoint getMaxDataPoint(int measure) {
        return new UnivariateDataPoint(getMaxTimestamp(measure), getMaxValue(measure));
    }

    default UnivariateDataPoint getFirstDataPoint(int measure) {
        return new UnivariateDataPoint(getFirstTimestamp(measure), getFirstValue(measure));
    }

    default UnivariateDataPoint getLastDataPoint(int measure) {
        return new UnivariateDataPoint(getLastTimestamp(measure), getLastValue(measure));
    }

    default String toString(int measure) {
        return "{" +
                "measure=" + measure +
                ", count=" + getCount() +
                ", sum=" + getSum(measure) +
                ", min=" + getMinValue(measure) +
                ", minTimestamp=" + getMinTimestamp(measure) +
                ", max=" + getMaxValue(measure) +
                ", maxTimestamp=" + getMaxTimestamp(measure) +
                ", first=" + getFirstValue(measure) +
                ", firstTimestamp=" + getFirstTimestamp(measure) +
                ", last=" + getLastValue(measure) +
                ", lastTimestamp=" + getLastTimestamp(measure) +
                ", average=" + getAverageValue(measure) +
                '}';
    }

}
