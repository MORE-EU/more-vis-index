package eu.more2020.visual.experiments.util;

import eu.more2020.visual.domain.Dataset.AbstractDataset;
import eu.more2020.visual.domain.Query.Query;
import eu.more2020.visual.datasource.InfluxDBQuery;
import eu.more2020.visual.datasource.SQLQuery;
import eu.more2020.visual.domain.TimeRange;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


import java.util.*;

import static eu.more2020.visual.experiments.util.UserOpType.*;


public class QuerySequenceGenerator {

    private static final Logger LOG = LogManager.getLogger(QuerySequenceGenerator.class);

    private float minShift;
    private float maxShift;

    private int minFilters;
    private int maxFilters;

    private float zoomFactor;

    private AbstractDataset dataset;

    private UserOpType opType;
    int seed = 42;

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

    public List<Query> generateQuerySequence(Query q0, int count) {
        Direction[] directions = Direction.getRandomDirections(count);
//        double[] shifts = ThreadLocalRandom.current().doubles(count, minShift, maxShift).toArray();
        double[] shifts = new Random(seed).doubles(count, minShift, maxShift).toArray();

        Random opRand = new Random(seed);
        List<UserOpType> ops = new ArrayList<>();

        int pans = 50;
        int zoom_in = 20;
        int zoom_out = 30;
        int resize = 1;

        for (int i = 0; i < pans; i++) ops.add(P);
        for (int i = 0; i < zoom_in; i++) ops.add(ZI);
        for (int i = 0; i < zoom_out; i++) ops.add(ZO);

        List<Query> queries = new ArrayList<>();
        queries.add(q0);
        Query q = q0;
        for (int i = 0; i < count - 1; i++) {
            opType = ops.get(opRand.nextInt(ops.size()));
            TimeRange timeRange = null;
            if (zoomFactor > 1 && opType.equals(ZI)) {
                timeRange = zoomIn(q);
            } else if (zoomFactor > 1 && opType.equals(ZO)) {
                timeRange = zoomOut(q);
            } else {
                timeRange = pan(q, shifts[i], directions[i]);
            }
            if(timeRange.getFrom() == q.getFrom() && timeRange.getTo() == q.getTo()){
                opType = ZI;
                timeRange = zoomIn(q);
            }
            q = new Query(timeRange.getFrom(), timeRange.getTo(), q0.getAccuracy(),
                    q0.getQueryMethod(), q0.getMeasures(),
                    q0.getViewPort(), opType);
            queries.add(q);
        }
        return queries;

    }

    private TimeRange pan(Query query, double shift, Direction direction) {
        long from = query.getFrom();
        long to = query.getTo();
        long timeShift = (long) ((to - from) * shift);

        switch (direction) {
            case L:
                if(dataset.getTimeRange().getFrom() >= (from - timeShift)){
                    opType = ZI;
                    return zoomIn(query);
                }
                to = to - timeShift;
                from = from - timeShift;
                break;
            case R:
                if(dataset.getTimeRange().getTo() <= (to + timeShift)){
                    opType = ZI;
                    return zoomIn(query);
                }
                to = to + timeShift;
                from = from + timeShift;
                break;
            default:
                return new TimeRange(from, to);

        }
        return new TimeRange(from, to);
    }


    private TimeRange zoomIn(Query query) {
        return zoom(query, 1f / zoomFactor);
    }

    private TimeRange zoomOut(Query query) {
        return zoom(query, zoomFactor);
    }

    private TimeRange zoom(Query query, float zoomFactor) {
        long from = query.getFrom();
        long to = query.getTo();
        float middle = (float) (from + to) / 2f;
        float size = (float) (to - from) * zoomFactor;
        long newFrom = (long) (middle - (size / 2f));
        long newTo = (long) (middle + (size / 2f));

        if (dataset.getTimeRange().getTo() < newTo) {
            newTo = dataset.getTimeRange().getTo();
        }
        if (dataset.getTimeRange().getFrom() > newFrom) {
            newFrom = dataset.getTimeRange().getFrom();
        }
        if (newFrom >= newTo) {
            newTo = dataset.getTimeRange().getTo();
            newFrom = dataset.getTimeRange().getFrom();
        }

        return new TimeRange(newFrom, newTo);
    }

}
