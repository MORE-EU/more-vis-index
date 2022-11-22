package eu.more2020.visual.domain;

import java.time.LocalDateTime;
import java.util.Arrays;

public class DataPoint {

    private LocalDateTime timestamp;

    private double[] values;

    public DataPoint(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public DataPoint(LocalDateTime timestamp, double[] values) {
        this.timestamp = timestamp;
        this.values = values;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public double[] getValues() {
        return values;
    }

    public void setValue(double[] values) {
        this.values = values;
    }

    @Override
    public String toString() {
        return "DataPoint{" +
            "timestamp=" + timestamp +
            ", values=" + Arrays.toString(values) +
            '}';
    }
}
