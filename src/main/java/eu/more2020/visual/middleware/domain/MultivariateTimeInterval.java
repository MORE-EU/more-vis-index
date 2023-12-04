package eu.more2020.visual.middleware.domain;


import java.util.List;

public class MultivariateTimeInterval implements TimeInterval{
    private TimeInterval interval;
    private List<Integer> measures;

    public MultivariateTimeInterval(TimeInterval interval, List<Integer> measures) {
        this.interval = interval;
        this.measures = measures;
    }

    public TimeInterval getInterval() {
        return interval;
    }

    public List<Integer> getMeasures() {
        return measures;
    }

    @Override
    public long getFrom() {
        return 0;
    }

    @Override
    public long getTo() {
        return 0;
    }

    @Override
    public String toString() {
        return "MultivariateTimeInterval{" +
                "interval=" + interval +
                ", measures=" + measures +
                '}';
    }
}