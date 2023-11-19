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

public class PostgreSQLAggregateDataPointsIterator implements Iterator<AggregatedDataPoint> {

    private static final Logger LOG = LoggerFactory.getLogger(DataSource.class);

    private final ResultSet resultSet;
    private final long from;
    private final long to;

    private final long aggregateInterval;
    private final int noOfGroups;
    private int k;
    private int unionGroup = 0;

    public PostgreSQLAggregateDataPointsIterator(long from, long to, ResultSet resultSet, int noOfGroups) throws SQLException {
        this.resultSet = resultSet;
        this.aggregateInterval = (to - from) / noOfGroups;
        this.from = from;
        this.to = to;
        this.noOfGroups = noOfGroups;
        resultSet.next();
        this.k = resultSet.getInt(2);
        this.unionGroup = resultSet.getInt(5); // signifies the union id
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

    /*
    Creates aggregate datapoints by using the k group index of the query.
    unionGroup represents a missing timeInterval group.
     */
    @Override
    public AggregatedDataPoint next() {
        int currentUnionGroup = unionGroup;
        int currentGroup = k;
        List<Integer> currentMeasures = new ArrayList<>();
        List<Double> currentValues = new ArrayList<>();
        try {
            while(currentGroup == k && currentUnionGroup == unionGroup) {
                int measure = resultSet.getInt(1);
                Double v_min = resultSet.getObject(3) == null ? null : resultSet.getDouble(3);
                Double v_max = resultSet.getObject(4) == null ? null : resultSet.getDouble(4);

                currentMeasures.add(measure);
                currentValues.add(v_min);
                currentValues.add(v_max);

                currentGroup = k;
                currentUnionGroup = unionGroup;

                resultSet.next();
                k = resultSet.getInt(2);
                unionGroup = resultSet.getInt(5); // signifies the union id
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        NonTimestampedStatsAggregator statsAggregator = new NonTimestampedStatsAggregator(currentMeasures);
        long firstTimestamp = from + currentGroup * aggregateInterval;
        long lastTimestamp = (currentGroup != noOfGroups) ? from + (currentGroup + 1) * aggregateInterval : to;

        for (int i = 0; i < currentMeasures.size(); i ++){
            Double value = currentValues.get(i);
            if(value != null) statsAggregator.accept(value, currentMeasures.get(i));
        }
        statsAggregator.setFrom(firstTimestamp);
        statsAggregator.setTo(lastTimestamp);
        LOG.debug("Created aggregate Datapoint {} - {} with measures {} with Agg {} at place {} ", DateTimeUtil.format(firstTimestamp), DateTimeUtil.format(lastTimestamp), statsAggregator.getMeasures(), aggregateInterval, currentGroup);
        return new ImmutableAggregatedDataPoint(firstTimestamp, lastTimestamp, statsAggregator);
    }
}
