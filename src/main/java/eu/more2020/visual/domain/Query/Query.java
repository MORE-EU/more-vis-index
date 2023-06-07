package eu.more2020.visual.domain.Query;

import eu.more2020.visual.domain.Aggregator;
import eu.more2020.visual.domain.TimeInterval;
import eu.more2020.visual.domain.ViewPort;
import eu.more2020.visual.experiments.util.UserOpType;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.List;

public class Query implements TimeInterval {

    final long from;
    final long to;
    List<Integer> measures;
    ViewPort viewPort;
    QueryMethod queryMethod;
    UserOpType opType;
    float accuracy;

    public Query(long from, long to, float accuracy, QueryMethod queryMethod, List<Integer> measures, ViewPort viewPort, UserOpType opType) {
        this.from = from;
        this.to = to;
        this.measures = measures;
        this.viewPort = viewPort;
        this.queryMethod = queryMethod;
        this.opType = opType;
        this.accuracy = accuracy;
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
        return getFromDate("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public String getFromDate(String format) {
        return Instant.ofEpochMilli(from).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    @Override
    public String getToDate() {
        return getToDate("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public String getToDate(String format) {
        return Instant.ofEpochMilli(to).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    public ViewPort getViewPort() {
        return viewPort;
    }

    public List<Integer> getMeasures() {
        return measures;
    }

    public UserOpType getOpType() {
        return opType;
    }

    public QueryMethod getQueryMethod() {
        return queryMethod;
    }

    public float getAccuracy() {
        return accuracy;
    }
}
