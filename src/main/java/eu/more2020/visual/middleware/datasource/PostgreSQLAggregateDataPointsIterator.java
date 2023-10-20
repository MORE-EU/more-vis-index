package eu.more2020.visual.middleware.datasource;

import eu.more2020.visual.middleware.domain.AggregatedDataPoint;
import eu.more2020.visual.middleware.domain.ImmutableAggregatedDataPoint;
import eu.more2020.visual.middleware.domain.NonTimestampedStatsAggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

public class PostgreSQLAggregateDataPointsIterator implements Iterator<AggregatedDataPoint> {

    private static final Logger LOG = LoggerFactory.getLogger(PostgreSQLAggregateDataPointsIterator.class);

    private final ResultSet resultSet;
    private final List<Integer> measures;
    private final long from;
    private final long to;

    private final long aggregateInterval;
    private final int noOfGroups;
    public PostgreSQLAggregateDataPointsIterator(long from, long to, List<Integer> measures, ResultSet resultSet, int noOfGroups){
        this.measures = measures;
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
        NonTimestampedStatsAggregator statsAggregator = new NonTimestampedStatsAggregator(measures);
        long firstTimestamp = from;
        int k = 0;
        try {
            for (int m : measures) {
                resultSet.next();
                int measure = resultSet.getInt(1);
                k = resultSet.getInt(2);
                Double v_min = resultSet.getObject(3) == null ? null : resultSet.getDouble(3);
                Double v_max = resultSet.getObject(4) == null ? null : resultSet.getDouble(4);
                firstTimestamp = from + k * aggregateInterval;
                if(v_min != null) {
                    statsAggregator.accept(v_min, measure);
                }
                if(v_max != null) {
                    statsAggregator.accept(v_max, measure);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        long lastTimestamp = (k != noOfGroups - 1) ? from + (k + 1) * aggregateInterval : to;
        statsAggregator.setFrom(firstTimestamp);
        statsAggregator.setTo(lastTimestamp);
//        LOG.debug("Created aggregate Datapoint {} - {} with Agg {} ", DateTimeUtil.format(firstTimestamp), DateTimeUtil.format(lastTimestamp), aggregateInterval);
        return new ImmutableAggregatedDataPoint(firstTimestamp, lastTimestamp, statsAggregator);
    }

}
