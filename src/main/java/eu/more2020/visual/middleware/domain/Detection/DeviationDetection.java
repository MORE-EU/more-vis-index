package eu.more2020.visual.middleware.domain.Detection;

import eu.more2020.visual.middleware.domain.Changepoint;
import eu.more2020.visual.middleware.domain.TimeRange;

import java.util.List;

public class DeviationDetection extends ChangepointDetection {


    private Integer weeks;

    public DeviationDetection() {
    }

    public DeviationDetection(TimeRange range, List<Changepoint> changepoints, Integer weeks) {
        this.range = range;
        this.changepoints = changepoints;
        this.weeks = weeks;
    }

    public Integer getWeeks() {
        return weeks;
    }

    public void setWeeks(Integer weeks) {
        this.weeks = weeks;
    }

    @Override
    public String toString() {
        return "DeviationDetection{" +
            "range=" + range +
            ", changepoints=" + changepoints +
            ", weeks=" + weeks +
            '}';
    }
}
