package eu.more2020.visual.datasource;

import eu.more2020.visual.domain.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class PostgreSQLAggregateDataPointsIterator implements Iterator<AggregatedDataPoint> {

    private final ResultSet resultSet;
    private final List<Integer> measures;
    private boolean changed = false;
    private int currentGroup = 0, group = 0;

    public PostgreSQLAggregateDataPointsIterator(List<Integer> measures, ResultSet resultSet){
        this.measures = measures;
        this.resultSet = resultSet;
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
        StatsAggregator statsAggregator = new StatsAggregator(measures);
        try {
            int i = 0;
           do  {
                int measureId = measures.indexOf(resultSet.getInt(1));
                long timestamp = resultSet.getLong(2);
                double value = resultSet.getDouble(3);
                group = resultSet.getInt(4);
                if(group != currentGroup) {
                    changed = true;
                    break;
                }
                if (i == 0) firstTimestamp = timestamp;
                UnivariateDataPoint point = new UnivariateDataPoint(timestamp, value);
                statsAggregator.accept(point, measureId);
                i++;
            } while(resultSet.next());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        currentGroup = group;
        return new ImmutableAggregatedDataPoint(firstTimestamp, statsAggregator);
    }


}
