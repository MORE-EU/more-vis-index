package eu.more2020.visual.datasource;

import eu.more2020.visual.domain.AggregatedDataPoint;
import eu.more2020.visual.domain.ImmutableAggregatedDataPoint;
import eu.more2020.visual.domain.StatsAggregator;
import eu.more2020.visual.domain.UnivariateDataPoint;
import eu.more2020.visual.util.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

public class PostgreSQLAggregateDataPointsIteratorM4 implements Iterator<AggregatedDataPoint> {

    private static final Logger LOG = LoggerFactory.getLogger(PostgreSQLAggregateDataPointsIteratorM4.class);

    private final ResultSet resultSet;
    private final List<Integer> measures;
    private final long from;
    private final long to;
    private boolean changed = false;
    private int currentGroup = -1, group = 0;

    private final long aggregateInterval;

    public PostgreSQLAggregateDataPointsIteratorM4(long from, long to, List<Integer> measures, ResultSet resultSet, int noOfGroups){
        this.measures = measures;
        this.resultSet = resultSet;
        this.aggregateInterval = (to - from) / noOfGroups;
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

    @Override
    public AggregatedDataPoint next() {
        long firstTimestamp = Long.MAX_VALUE;
        int i = 0;
        StatsAggregator statsAggregator = new StatsAggregator(measures);
        try {
            do {
                int measure = resultSet.getInt(1);
                long min_timestamp = resultSet.getLong(2);
                long max_timestamp = resultSet.getLong(3);
                double value = resultSet.getDouble(4);
                firstTimestamp = from + group * aggregateInterval;
                group = resultSet.getInt(5);
                currentGroup = currentGroup == -1 ? group : currentGroup;
                if (group != currentGroup) {
                    changed = true;
                    break;
                } else changed = false;
                UnivariateDataPoint point1 = new UnivariateDataPoint(min_timestamp, value);
                statsAggregator.accept(point1, measure);
                UnivariateDataPoint point2 = new UnivariateDataPoint(max_timestamp, value);
                statsAggregator.accept(point2, measure);
                i++;
            } while (resultSet.next());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        currentGroup = group;
        long lastTimestamp = hasNext() ? from + group * aggregateInterval : to;

//        LOG.debug("Created aggregate Datapoint {} - {} ", DateTimeUtil.format(firstTimestamp), DateTimeUtil.format(lastTimestamp));

        return new ImmutableAggregatedDataPoint(firstTimestamp, lastTimestamp, statsAggregator);
    }
}
