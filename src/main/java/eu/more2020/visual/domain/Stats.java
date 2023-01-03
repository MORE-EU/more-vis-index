package eu.more2020.visual.domain;

/**
 * A representation of aggregate statistics for multi-variate time series data points.
 */
public interface Stats {

    public int getCount();

    public double getSum(int measure);

    public double getMinValue(int measure);

    public double getMaxValue(int measure);

    public double getAverageValue(int measure);

    public long getMinTimestamp(int measure);

    public long getMaxTimestamp(int measure);
}
