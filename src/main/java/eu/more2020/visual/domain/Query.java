package eu.more2020.visual.domain;

import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;

public class Query {

    private TimeRange range;

    private HashMap<Integer, Double[]> filters;

    private List<Integer> measures;

    private int aggInterval;

    private ChronoUnit unit;

    private Aggregator aggregator;

    public Query() {
    }

    public Query(TimeRange range, List<Integer> measures, Aggregator aggregator, int aggInterval, ChronoUnit unit, HashMap<Integer, Double[]> filters) {
        this.range = range;
        this.measures = measures;
        this.aggInterval = aggInterval;
        this.unit = unit;
        this.filters = filters;
        this.aggregator = aggregator;
    }

    public TimeRange getRange() {
        return range;
    }

    public void setRange(TimeRange range) {
        this.range = range;
    }


    public HashMap<Integer, Double[]> getFilters() {
        return this.filters;
    }

    public void setFilters(HashMap<Integer, Double[]> filters) {
        this.filters = filters;
    }


    public List<Integer> getMeasures() {
        return measures;
    }

    public void setMeasures(List<Integer> measures) {
        this.measures = measures;
    }

    public int getAggInterval() {
        return aggInterval;
    }

    public void setAggInterval(int aggInterval) {
        this.aggInterval = aggInterval;
    }

    public ChronoUnit getUnit() {
        return unit;
    }

    public void setUnit(ChronoUnit unit) {
        this.unit = unit;
    }

    public Aggregator getAggregator() {
        return aggregator;
    }

    public void setAggregator(Aggregator aggregator) {
        this.aggregator = aggregator;
    }

    @Override
    public String toString() {
        return "Query{" +
                "range=" + range +
                ", filter=" + filters +
                ", measures=" + measures +
                ", aggInterval=" + aggInterval +
                ", unit=" + unit +
                ", aggregator=" + aggregator +
                '}';
    }
}
