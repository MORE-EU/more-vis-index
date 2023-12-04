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
import java.util.stream.Collectors;

public class PostgreSQLAggregateDataPointsIterator implements Iterator<AggregatedDataPoint> {

    private static final Logger LOG = LoggerFactory.getLogger(DataSource.class);

    private final ResultSet resultSet;
    private final long from;
    private final long to;

    private final long aggregateInterval;
    private final int noOfGroups;
    private int k;
    private int unionGroup;

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
            return !(resultSet.isAfterLast());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    /*
       For each grouping k that comes from the query.
       Get the values, put them in a list.
       Then pass through the list to initialize the corresponding aggregator.
       A unionGroup is a subQuery based on the UNION of the query.
     */
    @Override
    public AggregatedDataPoint next() {
        int currentUnionGroup = unionGroup;
        int currentGroup = k;
        List<Integer> currentMeasures = new ArrayList<>();
        List<Double> currentMinValues = new ArrayList<>();
        List<Double> currentMaxValues = new ArrayList<>();

        try {
            while(currentGroup == k && currentUnionGroup == unionGroup && hasNext()) {
                int measure = resultSet.getInt(1);
                Double v_min = resultSet.getObject(3) == null ? null : resultSet.getDouble(3);
                Double v_max = resultSet.getObject(4) == null ? null : resultSet.getDouble(4);

                currentMeasures.add(measure);
                currentMinValues.add(v_min);
                currentMaxValues.add(v_max);

                currentGroup = k;
                currentUnionGroup = unionGroup;
                if(!resultSet.next()) break;
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
            Double valueMin = currentMinValues.get(i);
            Double valueMax = currentMaxValues.get(i);
            if(valueMin != null) statsAggregator.accept(valueMin, currentMeasures.get(i));
            if(valueMax != null) statsAggregator.accept(valueMax, currentMeasures.get(i));

        }
        statsAggregator.setFrom(firstTimestamp);
        statsAggregator.setTo(lastTimestamp);
        LOG.debug("Created aggregate Datapoint {} - {} with measures {}  with firsts: {}, lasts: {},  with mins: {} and maxs: {} ",
                DateTimeUtil.format(firstTimestamp), DateTimeUtil.format(lastTimestamp), statsAggregator.getMeasures(),
                statsAggregator.getMeasures().stream().map(statsAggregator::getMinValue).collect(Collectors.toList()),
                statsAggregator.getMeasures().stream().map(statsAggregator::getMaxValue).collect(Collectors.toList()));
        return new ImmutableAggregatedDataPoint(firstTimestamp, lastTimestamp, statsAggregator);
    }
}
