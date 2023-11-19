package eu.more2020.visual.middleware.experiments.util;

import eu.more2020.visual.middleware.domain.Dataset.AbstractDataset;
import eu.more2020.visual.middleware.domain.Query.Query;
import eu.more2020.visual.middleware.domain.TimeRange;
import eu.more2020.visual.middleware.domain.ViewPort;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


import java.util.*;
import java.util.stream.Collectors;

import static eu.more2020.visual.middleware.experiments.util.UserOpType.*;


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

    double RESIZE_FACTOR = 1.2;

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


    public void addRandomElementToList(List<Integer> list1, List<Integer> list2) {
        Random random = new Random(seed);

        while (true) {
            // Generate a random index to select an element from list2
            int randomIndex = random.nextInt(list2.size());

            // Get the random element from list2
            Integer randomElement = list2.get(randomIndex);

            // Check if the element is not in list1
            if (!list1.contains(randomElement)) {
                // If it's not in list1, add it and exit the loop
                list1.add(randomElement);
                LOG.debug("Added element: " + randomElement);
                break;
            }
        }
    }

    public List<Query> generateQuerySequence(Query q0, int count) {
        Direction[] directions = Direction.getRandomDirections(count);
//        double[] shifts = ThreadLocalRandom.current().doubles(count, minShift, maxShift).toArray();
        double[] shifts = new Random(seed).doubles(count, minShift, maxShift).toArray();
        double[] zooms = new Random(seed).doubles(count, 1, zoomFactor).toArray();

        Random opRand = new Random(seed);
        List<UserOpType> ops = new ArrayList<>();

        int pans = 39;
        int zoom_in = 20;
        int zoom_out = 30;
        int resize = 1;
        int measure_change = 10;

        for (int i = 0; i < pans; i++) ops.add(P);
        for (int i = 0; i < zoom_in; i++) ops.add(ZI);
        for (int i = 0; i < zoom_out; i++) ops.add(ZO);
        for (int i = 0; i < measure_change; i++) ops.add(MC);
        for (int i = 0; i < resize; i++) ops.add(R);


        List<Query> queries = new ArrayList<>();
        queries.add(q0);
        Query q = q0;
        for (int i = 0; i < count - 1; i++) {
            zoomFactor = (float) zooms[i];
            opType = ops.get(opRand.nextInt(ops.size()));
            TimeRange timeRange = null;
            ViewPort viewPort = q.getViewPort();
            List<Integer> measures = q.getMeasures().stream().map(Integer::new).collect(Collectors.toList());
            if (zoomFactor > 1 && opType.equals(ZI)) {
                timeRange = zoomIn(q);
            } else if (zoomFactor > 1 && opType.equals(ZO)) {
                timeRange = zoomOut(q);
            } else if(opType.equals(P)){
                timeRange = pan(q, shifts[i], directions[i]);
            } else if(opType.equals(R)){
                viewPort = new ViewPort((int) (RESIZE_FACTOR * viewPort.getWidth()),
                        (int) (RESIZE_FACTOR * viewPort.getHeight()));
            } else if(opType.equals(MC) && measures.size() != dataset.getMeasures().size()){
                addRandomElementToList(measures, dataset.getMeasures());
            }
            if(timeRange == null) timeRange = new TimeRange(q.getFrom(), q.getTo());
            else if(timeRange.getFrom() == q.getFrom() && timeRange.getTo() == q.getTo()){
                opType = ZI;
                timeRange = zoomIn(q);
            }
            q = new Query(timeRange.getFrom(), timeRange.getTo(), q0.getAccuracy(), null,
                    q0.getQueryMethod(), measures, viewPort, opType);
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
