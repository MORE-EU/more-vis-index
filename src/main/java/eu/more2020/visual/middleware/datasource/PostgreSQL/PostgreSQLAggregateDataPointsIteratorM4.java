package eu.more2020.visual.middleware.datasource.PostgreSQL;

import eu.more2020.visual.middleware.datasource.DataSource;
import eu.more2020.visual.middleware.domain.*;
import eu.more2020.visual.middleware.util.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PostgreSQLAggregateDataPointsIteratorM4 implements Iterator<AggregatedDataPoint> {

    private static final Logger LOG = LoggerFactory.getLogger(DataSource.class);

    private final ResultSet resultSet;
    private final long from;
    private final long to;
    private final int noOfGroups;
    private final long aggregateInterval;

    private int k;
    private int unionGroup = 0;


    public PostgreSQLAggregateDataPointsIteratorM4(long from, long to,
                                                   ResultSet resultSet, int noOfGroups) {
        this.resultSet = resultSet;
        this.aggregateInterval = (to - from) / noOfGroups;
        this.from = from;
        this.to = to;
        this.noOfGroups = noOfGroups;

    }

    @Override
    public boolean hasNext() {
        try {
            return !(resultSet.isLast() || resultSet.isAfterLast());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }


    @Override
    public AggregatedDataPoint next() {
        int currentUnionGroup = unionGroup;
        int currentGroup = k;
        List<Integer> currentMeasures = new ArrayList<>();
        List<Integer> uniqueMeasures = new ArrayList<>();
        List<Double> currentValues = new ArrayList<>();
        List<Long> currentMinTimestamps = new ArrayList<>();
        List<Long> currentMaxTimestamps = new ArrayList<>();

        try {
            while(currentGroup == k && currentUnionGroup == unionGroup) {
                int measure = resultSet.getInt(1);
                Long t_min = resultSet.getObject(2) == null ? null : resultSet.getLong(2);
                Long t_max = resultSet.getObject(3) == null ? null : resultSet.getLong(3);
                Double value = resultSet.getObject(4) == null ? null : resultSet.getDouble(4);
                if(!uniqueMeasures.contains(measure)) uniqueMeasures.add(measure);
                currentMeasures.add(measure);

                currentMinTimestamps.add(t_min);
                currentMaxTimestamps.add(t_max);
                currentValues.add(value);
                currentValues.add(value);

                currentGroup = k;
                currentUnionGroup = unionGroup;
                LOG.info("{}", k);
                resultSet.next();
                k = resultSet.getInt(2);
                unionGroup = resultSet.getInt(5); // signifies the union id
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        LOG.info("");
        StatsAggregator statsAggregator = new StatsAggregator(uniqueMeasures);
        long firstTimestamp = from + currentGroup * aggregateInterval;
        long lastTimestamp = (currentGroup != noOfGroups) ? from + (currentGroup + 1) * aggregateInterval : to;

        for (int i = 0; i < currentMeasures.size(); i ++){
            Double value = currentValues.get(i);
            UnivariateDataPoint dataPoint1 = new UnivariateDataPoint(currentMinTimestamps.get(i), value);
            UnivariateDataPoint dataPoint2 = new UnivariateDataPoint(currentMaxTimestamps.get(i), value);
            statsAggregator.accept(dataPoint1, currentMeasures.get(i));
            statsAggregator.accept(dataPoint2, currentMeasures.get(i));
        }
        LOG.debug("Created aggregate Datapoint {} - {} with measures {} with Agg {} at place {} ", DateTimeUtil.format(firstTimestamp), DateTimeUtil.format(lastTimestamp), statsAggregator.getMeasures(), aggregateInterval, currentGroup);
        return new ImmutableAggregatedDataPoint(firstTimestamp, lastTimestamp, statsAggregator);
    }
}


//public class PostgreSQLAggregateDataPointsIteratorM4 implements Iterator<AggregatedDataPoint> {
//
//    private static final Logger LOG = LoggerFactory.getLogger(DataSource.class);
//
//    private final ResultSet resultSet;
//    private final List<List<Integer>> measures;
//    private final long from;
//    private final long to;
//    private boolean changed = false;
//    private int currentGroup = -1, group = 0;
//
//    private final List<TimeInterval> ranges;
//    private final long aggregateInterval;
//    private StatsAggregator statsAggregator;
//    public PostgreSQLAggregateDataPointsIteratorM4(long from, long to,
//                                                   List<TimeInterval> ranges,
//                                                   List<List<Integer>> measures,
//                                                   ResultSet resultSet, int noOfGroups) {
//        this.measures = measures;
//        this.resultSet = resultSet;
//        this.ranges = ranges;
//        this.aggregateInterval = (to - from) / noOfGroups;
//        this.from = from;
//        this.to = to;
//    }
//
//    @Override
//    public boolean hasNext() {
//        try {
//            if (resultSet.isAfterLast()) return false;
//            if (changed) return true;
//            return resultSet.next();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        return false;
//    }
//
//    public StatsAggregator getStatsAggregator(long timestamp){
//        int index = 0;
//        for(TimeInterval range : ranges){
//            if(timestamp < range.getTo()) break;
//            else index ++;
//        }
//        return new StatsAggregator(measures.get(index));
//    }
//
//    @Override
//    public AggregatedDataPoint next() {
//        long firstTimestamp = Long.MAX_VALUE;
//        int i = 0;
//        try {
//            statsAggregator = getStatsAggregator(resultSet.getLong(3));
//            do {
//                int measure = resultSet.getInt(1);
//                long min_timestamp = resultSet.getLong(2);
//                long max_timestamp = resultSet.getLong(3);
//                Double value = resultSet.getObject(4) == null ? null : resultSet.getDouble(4);
//                firstTimestamp = from + group * aggregateInterval;
//                group = resultSet.getInt(5);
//                currentGroup = currentGroup == -1 ? group : currentGroup;
//                if (group != currentGroup) {
//                    changed = true;
//                    break;
//                } else changed = false;
//                if (value != null) {
//                    UnivariateDataPoint point1 = new UnivariateDataPoint(min_timestamp, value);
//                    statsAggregator.accept(point1, measure);
//                    UnivariateDataPoint point2 = new UnivariateDataPoint(max_timestamp, value);
//                    statsAggregator.accept(point2, measure);
//                }
//                i++;
//            } while (resultSet.next());
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        currentGroup = group;
//        long lastTimestamp = hasNext() ? from + group * aggregateInterval : to;
//        LOG.debug("Created aggregate Datapoint {} - {} ", DateTimeUtil.format(firstTimestamp), DateTimeUtil.format(lastTimestamp));
//        return new ImmutableAggregatedDataPoint(firstTimestamp, lastTimestamp, statsAggregator);
//    }
//}
