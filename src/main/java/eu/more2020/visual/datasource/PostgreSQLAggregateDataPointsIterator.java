package eu.more2020.visual.datasource;

import eu.more2020.visual.domain.*;
import org.apache.log4j.LogMF;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class PostgreSQLAggregateDataPointsIterator implements Iterator<AggregatedDataPoint> {

    private final ResultSet resultSet;
    private final List<Integer> measures;
    private boolean changed = false;
    private int currentGroup = -1, group = 0;

    private AggregateInterval aggregateInterval;

    public PostgreSQLAggregateDataPointsIterator(List<Integer> measures, ResultSet resultSet, AggregateInterval aggregateInterval){
        this.measures = measures;
        this.resultSet = resultSet;
        this.aggregateInterval = aggregateInterval;
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

    @Override
    public AggregatedDataPoint next() {
        long firstTimestamp = 0L;
        long lastTimestamp = 0L;
        int i = 0;
        StatsAggregator statsAggregator = new StatsAggregator(measures);
        try {
           do  {
                int measure = resultSet.getInt(1);
                long timestamp = resultSet.getLong(2);
                double value = resultSet.getDouble(3);
                group = resultSet.getInt(4);
                currentGroup = currentGroup == -1 ? group : currentGroup;
                if(group != currentGroup) {
                    changed = true;
                    break;
                }
                else changed = false;
                if (i == 0)  firstTimestamp = timestamp;
                lastTimestamp = timestamp;
                UnivariateDataPoint point = new UnivariateDataPoint(timestamp, value);
                statsAggregator.accept(point, measure);
                i++;
            } while(resultSet.next());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        currentGroup = group;
        return new ImmutableAggregatedDataPoint(firstTimestamp, lastTimestamp, statsAggregator);
    }

}
