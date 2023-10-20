package eu.more2020.visual.middleware.domain;

import java.io.Serializable;

public class Changepoint implements Serializable {

    private Integer id;
    private TimeRange range;
    private double score;

    public Changepoint() {
    }

    public Changepoint(Integer id, TimeRange range, double score) {
        this.id = id;
        this.range = range;
        this.score = score;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public TimeRange getRange() {
        return range;
    }

    public void setRange(TimeRange range) {
        this.range = range;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return "Changepoint{" +
            "id=" + id +
            ", range=" + range +
            ", score=" + score +
            '}';
    }
}
