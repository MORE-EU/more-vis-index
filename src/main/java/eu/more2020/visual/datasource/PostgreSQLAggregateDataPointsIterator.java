package eu.more2020.visual.datasource;

import eu.more2020.visual.domain.*;
import eu.more2020.visual.util.DateTimeUtil;
import org.apache.log4j.LogMF;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class PostgreSQLAggregateDataPointsIterator implements Iterator<AggregatedDataPoint> {

    private final ResultSet resultSet;
    private final List<Integer> measures;
    private final long from;
    private final long to;
    private boolean changed = false;
    private int currentGroup = -1, group = 0;

    private int noOfGroups;

    public PostgreSQLAggregateDataPointsIterator(long from, long to, List<Integer> measures, ResultSet resultSet, int noOfGroups){
        this.measures = measures;
        this.resultSet = resultSet;
        this.noOfGroups = noOfGroups;
        this.from = from;
        this.to = to;
    }

    @Override
    public boolean hasNext() {
        try {
            if(resultSet.isAfterLast()) return false;
            if(changed) return true;
            return resultSet.next();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

//    @Override
//    public AggregatedDataPoint next() {
//        long firstTimestamp = Long.MAX_VALUE;
//        long lastTimestamp = 0L;
//        int i = 0;
//        StatsAggregator statsAggregator = new StatsAggregator(measures);
//        try {
//           do  {
//                int measure = resultSet.getInt(1);
//                long min_timestamp = resultSet.getLong(2);
//                long max_timestamp = resultSet.getLong(3);
//                double value = resultSet.getDouble(4);
//                group = resultSet.getInt(5);
//                currentGroup = currentGroup == -1 ? group : currentGroup;
//                if(group != currentGroup) {
//                    changed = true;
//                    break;
//                }
//                else changed = false;
//                if (i == 0)  firstTimestamp = Math.min(min_timestamp, firstTimestamp);
//                lastTimestamp = Math.max(max_timestamp, lastTimestamp);
//                UnivariateDataPoint point1 = new UnivariateDataPoint(min_timestamp, value);
//                statsAggregator.accept(point1, measure);
//                UnivariateDataPoint point2 = new UnivariateDataPoint(max_timestamp, value);
//                statsAggregator.accept(point2, measure);
//                i++;
//            } while(resultSet.next());
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        currentGroup = group;
//        return new ImmutableAggregatedDataPoint(firstTimestamp, lastTimestamp, statsAggregator);
//    }

    @Override
    public AggregatedDataPoint next() {
        StatsAggregator statsAggregator = new StatsAggregator(measures);
        long firstTimestamp = from;
        long aggregateInterval = (to - from) / noOfGroups;
        int k = 0;
        try {
            for (int m : measures) {
                int measure = resultSet.getInt(1);
                k = resultSet.getInt(2);
                double v_min = resultSet.getDouble(3);
                double v_max = resultSet.getDouble(4);
                firstTimestamp = from + k * aggregateInterval;
                UnivariateDataPoint point1 = new UnivariateDataPoint(firstTimestamp, v_min);
                statsAggregator.accept(point1, measure);
                UnivariateDataPoint point2 = new UnivariateDataPoint(firstTimestamp, v_max);
                statsAggregator.accept(point2, measure);
                resultSet.next();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        long lastTimestamp = hasNext() ? from + (k + 1) * aggregateInterval : to;
        return new ImmutableAggregatedDataPoint(firstTimestamp, lastTimestamp, statsAggregator);
    }

}
