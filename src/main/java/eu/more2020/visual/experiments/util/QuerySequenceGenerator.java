package eu.more2020.visual.experiments.util;

import com.google.common.collect.Range;

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

    private int minShift;
    private int maxShift;

    private int minFilters;
    private int maxFilters;

    private float zoomFactor;

    public QuerySequenceGenerator(int minShift, int maxShift, int minFilters, int maxFilters, float zoomFactor) {
        this.minShift = minShift;
        this.maxShift = maxShift;
        this.minFilters = minFilters;
        this.maxFilters = maxFilters;
        this.zoomFactor = zoomFactor;
    }

    public List<Query> generateQuerySequence(Query q0, int count) {
        Direction[] directions = Direction.getRandomDirections(count);
        int[] shifts = new Random(0).ints(count, minShift, maxShift + 1).toArray();
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
//                timeRange = pan(query, shifts[i], directions[i]);
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

//    private Rectangle pan(Query query, int shift, Direction direction) {
//        Range<Float> xRange = query.getRect().getXRange();
//        Range<Float> yRange = query.getRect().getYRange();
//        shift = Math.abs(shift);
//
//        switch (direction) {
//            case N:
//            case NE:
//            case NW:
//                yRange = adjustRange(yRange, shift);
//                break;
//            case S:
//            case SE:
//            case SW:
//                yRange = adjustRange(yRange, -shift);
//        }
//        switch (direction) {
//            case E:
//            case NE:
//            case SE:
//                xRange = adjustRange(xRange, shift);
//                break;
//            case W:
//            case NW:
//            case SW:
//                xRange = adjustRange(xRange, -shift);
//        }
//        return new Rectangle(xRange, yRange);
//    }


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
        long newFrom =  (long) (from - (zoomFactor * (middle - from)));
        long newTo =  (long) (to + (zoomFactor * (to - middle)));
        return new TimeRange(newFrom, newTo);
    }

    private Range<Float> adjustRange(Range<Float> range, int shift) {
        float interval = (range.upperEndpoint() -
                range.lowerEndpoint()) * shift / 100;
        return Range.open(range.lowerEndpoint() + interval, range.upperEndpoint() + interval);
    }

}
