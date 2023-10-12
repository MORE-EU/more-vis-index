package eu.more2020.visual.index.domain.Detection;

import eu.more2020.visual.index.domain.Changepoint;
import eu.more2020.visual.index.domain.TimeRange;

import java.util.List;

public class ChangepointDetection extends AbstractDetection {

    TimeRange range;

    List<Changepoint> changepoints;


    public ChangepointDetection() {}

    public ChangepointDetection(TimeRange range, List<Changepoint> changepoints) {
        this.range = range;
        this.changepoints = changepoints;
    }


    public List<Changepoint> getChangepoints() {
        return changepoints;
    }

    public void setChangepoints(List<Changepoint> changepoints) {
        this.changepoints = changepoints;
    }


    @Override
    public String toString() {
        return "ChangepointDetection{" +
            "range=" + range +
            ", changepoints=" + changepoints +
            '}';
    }
}
