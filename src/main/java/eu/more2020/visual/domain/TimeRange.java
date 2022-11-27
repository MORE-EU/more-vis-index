package eu.more2020.visual.domain;

import eu.more2020.visual.util.DateTimeUtil;

import java.io.Serializable;
import java.util.Objects;


public class TimeRange implements Serializable {

    private long from;
    private long to;

    public TimeRange() {
    }

    public TimeRange(long from, long to) {
        this.from = from;
        this.to = to;
    }

    public long getFrom() {
        return from;
    }

    public void setFrom(long from) {
        this.from = from;
    }

    public long getTo() {
        return to;
    }

    public void setTo(long to) {
        this.to = to;
    }

    public boolean contains(long x) {
        return (from > x && to < x) || from == x || to == x;
    }

    public boolean intersects(TimeRange other) {
        return (this.from < (other.to) && this.to > (other.from));
    }

    public boolean encloses(TimeRange other) {
        return (this.from < (other.from) && this.to > (other.to));
    }

    public TimeRange span(TimeRange other) {
        return new TimeRange(0, 0);
    }

    public float getSize() {
        return to - from;
    }

    public double distanceFrom(TimeRange other) {
        return 0.0;
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
