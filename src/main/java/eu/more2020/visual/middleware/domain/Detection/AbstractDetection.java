package eu.more2020.visual.middleware.domain.Detection;

import eu.more2020.visual.middleware.domain.TimeRange;

public abstract class AbstractDetection {

    String id;

    TimeRange range;

    public final String getId() {
        return id;
    }

    public final void setId(String id) {
        this.id = id;
    }

    public final TimeRange getRange() {
        return range;
    }

    public final void setRange(TimeRange range) {
        this.range = range;
    }

    @Override
    public String toString() {
        return "AbstractDetection{" +
            "id=" + id +
            "range=" + range +
            '}';
    }
}
