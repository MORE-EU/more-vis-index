package eu.more2020.visual.index.domain.Query;

import eu.more2020.visual.index.domain.TimeInterval;
import eu.more2020.visual.index.domain.ViewPort;
import eu.more2020.visual.index.experiments.util.UserOpType;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;

public class IndexQuery implements TimeInterval {

    private long from;
    private long to;
    private List<Integer> measures;
    private HashMap<Integer, Double[]> filter;

    private String frequency;

    public IndexQuery() {
    }

    public IndexQuery(long from, long to, List<Integer> measures,
                      String frequency, HashMap<Integer, Double[]> filter) {
        this.from = from;
        this.to = to;
        this.measures = measures;
        this.frequency = frequency;
        this.filter = filter;
    }


    public String getFrequency() {
        return frequency;
    }

    public void setFrequency(String frequency) {
        this.frequency = frequency;
    }

    public List<Integer> getMeasures() {
        return measures;
    }

    public void setMeasures(List<Integer> measures) {
        this.measures = measures;
    }

    public HashMap<Integer, Double[]> getFilter() {
        return filter;
    }

    public void setFilter(HashMap<Integer, Double[]> filter) {
        this.filter = filter;
    }

    @Override
    public String toString() {
        return "Query{" +
                ", from=" + from +
                ", to =" + to +
                ", measures=" + measures +
                ", frequency='" + frequency + '\'' +
                '}';
    }


    @Override
    public long getFrom() {
        return from;
    }

    @Override
    public long getTo() {
        return to;
    }
}