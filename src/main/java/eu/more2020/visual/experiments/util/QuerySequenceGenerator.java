package eu.more2020.visual.experiments.util;

import eu.more2020.visual.domain.Dataset.AbstractDataset;
import eu.more2020.visual.domain.Query.AbstractQuery;
import eu.more2020.visual.domain.Query.InfluxDBQuery;
import eu.more2020.visual.domain.Query.Query;
import eu.more2020.visual.domain.Query.SQLQuery;
import eu.more2020.visual.domain.TimeRange;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static eu.more2020.visual.experiments.util.UserOpType.*;


public class QuerySequenceGenerator {

    private static final Logger LOG = LogManager.getLogger(QuerySequenceGenerator.class);

    private float minShift;
    private float maxShift;

    private int minFilters;
    private int maxFilters;

    private float zoomFactor;

    private AbstractDataset dataset;

    int seed = 0;

    public QuerySequenceGenerator(float minShift, float maxShift, float zoomFactor, AbstractDataset dataset) {
        this.minShift = minShift;
        this.maxShift = maxShift;
        this.zoomFactor = zoomFactor;
        this.dataset = dataset;
    }

    public QuerySequenceGenerator(float minShift, float maxShift, int minFilters, int maxFilters, float zoomFactor, AbstractDataset dataset) {
        this.minShift = minShift;
        this.maxShift = maxShift;
        this.minFilters = minFilters;
        this.maxFilters = maxFilters;
        this.zoomFactor = zoomFactor;
        this.dataset = dataset;
    }

    public List<AbstractQuery> generateQuerySequence(Query q0, int count) {
        Direction[] directions = Direction.getRandomDirections(count);
//        double[] shifts = ThreadLocalRandom.current().doubles(count, minShift, maxShift).toArray();
        double[] shifts = new Random(seed).doubles(count, minShift, maxShift).toArray();

        Random opRand = new Random(seed);
        List<UserOpType> ops = new ArrayList<>();
        int pans = 50;
        int zoom_in = 25;
        int zoom_out = 25;
        int resize = 1;

        for (int i = 0; i < pans; i++) ops.add(P);
        for (int i = 0; i < zoom_in; i++) ops.add(ZI);
        for (int i = 0; i < zoom_out; i++) ops.add(ZO);

        List<AbstractQuery> queries = new ArrayList<>();

        List<String> measureNames = q0.getMeasures().size() < dataset.getHeader().length ?
                q0.getMeasures().stream().map(m -> dataset.getHeader()[m]).collect(Collectors.toList()) : null;
        queries.add(q0);
        queries.add(new SQLQuery(q0.getFrom(), q0.getTo(), q0.getMeasures(), q0.getViewPort()));
        queries.add(new InfluxDBQuery(q0.getFrom(), q0.getTo(), measureNames, q0.getViewPort()));
        Query ttiQuery = q0;
        for (int i = 0; i < count - 1; i++) {
            UserOpType opType = ops.get(opRand.nextInt(ops.size()));
            TimeRange timeRange = null;

            if (zoomFactor > 1 && opType.equals(ZI)) {
                timeRange = zoomIn(ttiQuery);
            } else if (zoomFactor > 1 && opType.equals(ZO)) {
                timeRange = zoomOut(ttiQuery);
            } else {
                timeRange = pan(ttiQuery, shifts[i], directions[i]);
            }

            ttiQuery = new Query(timeRange.getFrom(), timeRange.getTo(), q0.getAccuracy(),
                    q0.getQueryMethod(), q0.getMeasures(),
                    q0.getViewPort(), opType);
            SQLQuery sqlQuery = new SQLQuery(timeRange.getFrom(), timeRange.getTo(), q0.getMeasures(), q0.getViewPort());
            InfluxDBQuery influxDBQuery = new InfluxDBQuery(timeRange.getFrom(), timeRange.getTo(), measureNames, q0.getViewPort());

            queries.add(ttiQuery);
            queries.add(sqlQuery);
            queries.add(influxDBQuery);
        }
        return queries;

    }

    private TimeRange pan(Query query, double shift, Direction direction) {
        long from = query.getFrom();
        long to = query.getTo();
        long timeShift = (long) ((to - from) * shift);

        switch (direction) {
            case L:
                if(dataset.getTimeRange().getFrom() > (from - timeShift)) break;
                from = from - timeShift;
                to = to - timeShift;
                break;
            case R:
                if(dataset.getTimeRange().getTo() < (to + timeShift)) break;
                to = to + timeShift;
                from = from + timeShift;
                break;
            default:
                return new TimeRange(from, to);

        }
        return new TimeRange(from, to);
    }


    private TimeRange zoomOut(Query query) {
        return zoom(query, zoomFactor);
    }

    private TimeRange zoomIn(Query query) {
        return zoom(query, 1f / zoomFactor);
    }

    private TimeRange zoom(Query query, float zoomFactor) {
        long from = query.getFrom();
        long to = query.getTo();
        float middle = (from + to) / 2f;
        float size = (from - to) * zoomFactor;
        long newFrom = (long) (middle + (size / 2f));
        long newTo =  (long) (middle - (size / 2f));
        if(dataset.getTimeRange().getTo() < newTo) newTo = dataset.getTimeRange().getTo();
        if(dataset.getTimeRange().getFrom() > newFrom) newFrom = dataset.getTimeRange().getFrom();
        if(newFrom == newTo){
            newTo = dataset.getTimeRange().getTo();
            newFrom = dataset.getTimeRange().getFrom();
        }
        return new TimeRange(newFrom, newTo);
    }
}
