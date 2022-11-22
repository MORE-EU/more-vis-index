package eu.more2020.visual.domain;

import java.util.HashMap;
import java.util.List;

public class Query {

    private TimeRange range;

    private HashMap<Integer, Double[]> filter;

    private List<Integer> measures;

    private String frequency;

    public Query() {
    }

    public Query(TimeRange range, List<Integer> measures, String frequency, HashMap<Integer, Double[]> filter) {
        this.range = range;
        this.measures = measures;
        this.frequency = frequency;
        this.filter = filter;
    }

    public TimeRange getRange() {
        return range;
    }

    public void setRange(TimeRange range) {
        this.range = range;
    }

    public String getFrequency() {
        return frequency;
    }

    public HashMap<Integer, Double[]> getFilter() {
        return this.filter;
    }

    public void setFilter(HashMap<Integer, Double[]> filter) {
        this.filter = filter;
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


    @Override
    public String toString() {
        return "Query{" +
            "range=" + range +
            ", measures=" + measures +
            ", frequency=" + frequency + 
            ", filter=" + (filter != null ? filter.toString() : "null") +
            '}';
    }
}
