package eu.more2020.visual.experiments.util;

import com.google.common.collect.Range;

import eu.more2020.visual.domain.Dataset.AbstractDataset;
import eu.more2020.visual.domain.Query;
import eu.more2020.visual.domain.TimeRange;
import org.apache.commons.math3.util.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


import java.sql.Time;
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
        double[] shifts = new Random(0).doubles(count, minShift, maxShift + 1).toArray();
        int[] filterCounts = new Random(0).ints(count, minFilters, maxFilters + 1).toArray();


        Random opRand = new Random(0);
        List<UserOpType> ops = Arrays.asList(new UserOpType[]{P, P, ZI, ZO});


        Random randomFilterValueGen = new Random(0);
        List<Query> queries = new ArrayList<>();
        queries.add(q0);
        Query query = q0;
        for (int i = 0; i < count - 1; i++) {
            UserOpType opType = ops.get(opRand.nextInt(ops.size()));
            TimeRange timeRange = null;

            if (zoomFactor > 1 && opType.equals(ZI)) {
                timeRange = zoomIn(query);
            } else if (zoomFactor > 1 && opType.equals(ZO)) {
                timeRange = zoomOut(query);
            } else {
                timeRange = pan(query, shifts[i], directions[i]);
            }

            Map<Integer, String> filters = new HashMap<>();
            int filterCount = filterCounts[i];

//            while (filterCount > 0) {
//                CategoricalColumn column = colDistribution.sample();
//                if (!filters.containsKey(column.getIndex())) {
//                    String filterValue = column.getValue((short) randomFilterValueGen.nextInt(column.getCardinality()));
//                    filters.put(column.getIndex(), filterValue);
//                    filterCount--;
//                }
//            }
            query = new Query(timeRange.getFrom(), timeRange.getTo(), null, null, null);
            queries.add(query);
        }
        return queries;

    }

    private TimeRange pan(Query query, double shift, Direction direction) {
        long from = query.getFrom();
        long to = query.getTo();
        shift = Math.abs(shift);
        int timeShift = (int) ((to - from) * shift);
        switch (direction) {
            case L:
                if(dataset.getTimeRange().getFrom() > from - timeShift) break;
                from = from - from * timeShift;
                to = to - timeShift;
                break;
            case R:
                if(dataset.getTimeRange().getTo() < to + timeShift) break;
                to = to + timeShift;
                from = from + timeShift;
                break;

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
        return new TimeRange(newFrom, newTo);
    }
}
