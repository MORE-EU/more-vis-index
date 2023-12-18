package eu.more2020.visual.middleware.domain;

public class ImmutableMultivariateDataPoint implements MultiVariateDataPoint {

    long timestamp;
    double[] values;

    
    public ImmutableMultivariateDataPoint(long timestamp, double[] values) {
        this.timestamp = timestamp;
        this.values = values;
    }


    @Override
    public long getTimestamp() {
        return timestamp;
    }
    @Override
    public double[] getValues() {
        return values;
    }
}
