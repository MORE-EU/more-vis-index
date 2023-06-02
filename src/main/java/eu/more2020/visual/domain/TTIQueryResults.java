package eu.more2020.visual.domain;

import eu.more2020.visual.index.TimeSeriesSpan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class TTIQueryResults {
    private List<TimeSeriesSpan> overlappingIntervals;
    private List<TimeRange> missingIntervals;
    private long from;
    private long to;

    public TTIQueryResults(long from, long to, List<TimeSeriesSpan> overlappingIntervals, List<TimeRange> missingIntervals) {
        this.from = from;
        this.to = to;
        this.overlappingIntervals = overlappingIntervals;
        this.missingIntervals = missingIntervals;
        overlappingIntervals.sort((i1, i2) -> (int) (i1.getFrom() - i2.getFrom())); // Sort intervals
    }

    public List<TimeSeriesSpan> getOverlappingIntervals() {
        return overlappingIntervals;
    }

    public List<TimeRange> getMissingIntervals() {
        return missingIntervals;
    }

    public boolean addAll(List<TimeSeriesSpan> timeSeriesSpans){
        boolean added = overlappingIntervals.addAll(timeSeriesSpans);
        if(!added) return false;
        overlappingIntervals.sort(Comparator.comparing(TimeSeriesSpan::getFrom)); // Sort intervals
        missingIntervals = new ArrayList<>();
        return true;
    }

    public long getFrom() {
        return from;
    }

    public long getTo() {
        return to;
    }
}