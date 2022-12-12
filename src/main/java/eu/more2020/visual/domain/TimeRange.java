package eu.more2020.visual.domain;

import eu.more2020.visual.util.DateTimeUtil;

import java.io.Serializable;
import java.util.Objects;


public class TimeRange implements Serializable, TimeInterval {

    private final long from;
    private final long to;

    public TimeRange(final long from, final long to) {
        this.from = from;
        this.to = to;
    }

    @Override
    public long getFrom() {
        return from;
    }

    @Override
    public long getTo() {
        return to;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeRange range = (TimeRange) o;
        return Objects.equals(from, range.from) &&
                Objects.equals(to, range.to);
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, to);
    }

    @Override
    public String toString() {
        return "TimeRange{" +
                "from=" + DateTimeUtil.format(from) +
                ", to=" + DateTimeUtil.format(to) +
                '}';
    }
}
