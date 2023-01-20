package eu.more2020.visual.domain;

import eu.more2020.visual.util.DateTimeUtil;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


public class TimeRange implements Serializable, TimeInterval {

    private final long from;
    private final long to;

    public TimeRange(final long from, final long to) {
        this.from = from;
        this.to = to;
    }

    public TimeRange getOverlap(TimeRange other){
        long overlapFrom = Math.max(from, other.from);
        long overlapTo = Math.min(to, other.to);
        return new TimeRange(overlapFrom, overlapTo);
    }

    public List<TimeRange> diff(TimeInterval other) {
        List<TimeRange> diffs = new ArrayList<>();
        if(getFrom() > other.getFrom()) diffs.add(new TimeRange(other.getFrom(), getFrom()));
        if(getTo() < other.getTo()) diffs.add(new TimeRange(getTo(), other.getTo()));
        return diffs;
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
    public String getFromDate() {
        return Instant.ofEpochMilli(from).atZone(ZoneId.of("Europe/Athens")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    @Override
    public String getToDate() {
        return Instant.ofEpochMilli(to).atZone(ZoneId.of("Europe/Athens")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
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
                "from=" + getFromDate() +
                ", to=" + getToDate() +
                ", " + from  + " " + to +
                '}';
    }
}