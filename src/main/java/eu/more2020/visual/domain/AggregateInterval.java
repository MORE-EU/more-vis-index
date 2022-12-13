package eu.more2020.visual.domain;

import java.time.temporal.ChronoUnit;

public class AggregateInterval {
    private long interval;
    private ChronoUnit chronoUnit;

    public AggregateInterval(long interval, ChronoUnit chronoUnit) {
        this.interval = interval;
        this.chronoUnit = chronoUnit;
    }

    public long getInterval() {
        return interval;
    }

    public ChronoUnit getChronoUnit() {
        return chronoUnit;
    }

    @Override
    public String toString() {
        return "AggregateInterval{" +
                 interval +
                 " " +
                 chronoUnit +
                '}';
    }
}
