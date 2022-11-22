package eu.more2020.visual.domain;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


public class TimeRange implements Serializable {

    private LocalDateTime from;
    private LocalDateTime to;

    public TimeRange() {
    }

    public TimeRange(LocalDateTime from, LocalDateTime to) {
        this.from = from;
        this.to = to;
    }

    public LocalDateTime getFrom() {
        return from;
    }

    public void setFrom(LocalDateTime from) {
        this.from = from;
    }

    public LocalDateTime getTo() {
        return to;
    }

    public void setTo(LocalDateTime to) {
        this.to = to;
    }

    public boolean contains(LocalDateTime x) {
        return (from.isBefore(x) && to.isAfter(x)) || from.isEqual(x) || to.isEqual(x);
    }

    public boolean intersects(TimeRange other) {
        return (this.from.isBefore(other.to) && this.to.isAfter(other.from));
    }

    public boolean encloses(TimeRange other) {
        return (this.from.isBefore(other.from) && this.to.isAfter(other.to));
    }

    public TimeRange span(TimeRange other) {
        int fromCmp = from.compareTo(other.from);
        int toCmp = to.compareTo(other.to);
        if (fromCmp <= 0 && toCmp >= 0) {
            return this;
        } else if (fromCmp >= 0 && toCmp <= 0) {
            return other;
        } else {
            LocalDateTime newFrom = (fromCmp <= 0) ? from : other.from;
            LocalDateTime newTo = (toCmp >= 0) ? to : other.to;
            return new TimeRange(newFrom, newTo);
        }
    }

    public float getSize() {
        return to.getNano() - from.getNano();
    }


    public List toList() {
        List<LocalDateTime> list = new ArrayList<>(2);
        list.add(this.from);
        list.add(this.to);
        return list;
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
            "from=" + from +
            ", to=" + to +
            '}';
    }
}
