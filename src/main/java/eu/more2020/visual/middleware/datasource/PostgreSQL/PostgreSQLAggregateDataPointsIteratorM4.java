package eu.more2020.visual.middleware.datasource.PostgreSQL;

import eu.more2020.visual.middleware.datasource.DataSource;
import eu.more2020.visual.middleware.domain.*;
import eu.more2020.visual.middleware.util.DateTimeUtil;
import org.apache.arrow.flatbuf.Int;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PostgreSQLAggregateDataPointsIteratorM4 implements Iterator<AggregatedDataPoint> {

    private static final Logger LOG = LoggerFactory.getLogger(DataSource.class);

    private final ResultSet resultSet;
    private final long from;
    private final long to;
    private final Map<Integer, Integer> noOfGroups;
//    private final long aggregateInterval;

    private int k;
    private int unionGroup = 0;


    public PostgreSQLAggregateDataPointsIteratorM4(long from, long to,
                                                   ResultSet resultSet, Map<Integer, Integer> noOfGroups) throws SQLException {
        this.resultSet = resultSet;
//        this.aggregateInterval = (to - from) / noOfGroups;
        this.from = from;
        this.to = to;
        this.noOfGroups = noOfGroups;
        resultSet.next();
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
//        int currentUnionGroup = unionGroup;
//        int currentGroup = k;
//        List<Integer> currentMeasures = new ArrayList<>();
//        List<Integer> uniqueMeasures = new ArrayList<>();
//        List<Double> currentValues = new ArrayList<>();
//        List<Long> currentMinTimestamps = new ArrayList<>();
//        List<Long> currentMaxTimestamps = new ArrayList<>();
//
//        try {
//            while(currentGroup == k && currentUnionGroup == unionGroup && hasNext()) {
//                int measure = resultSet.getInt(1);
//                Long t_min = resultSet.getObject(2) == null ? null : resultSet.getLong(2);
//                Long t_max = resultSet.getObject(3) == null ? null : resultSet.getLong(3);
//                Double value = resultSet.getObject(4) == null ? null : resultSet.getDouble(4);
//                if(!uniqueMeasures.contains(measure)) uniqueMeasures.add(measure);
//                currentMeasures.add(measure);
//
//                currentMinTimestamps.add(t_min);
//                currentMaxTimestamps.add(t_max);
//                currentValues.add(value);
//
//                currentGroup = k;
//                currentUnionGroup = unionGroup;
//                if(!resultSet.next()) break;
//                k = resultSet.getInt(5);
//                unionGroup = resultSet.getInt(6); // signifies the union id
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        StatsAggregator statsAggregator = new StatsAggregator(uniqueMeasures);
//        long firstTimestamp = from + currentGroup * aggregateInterval;
//        long lastTimestamp = (currentGroup != noOfGroups) ? from + (currentGroup + 1) * aggregateInterval : to;
//
//        for (int i = 0; i < currentMeasures.size(); i ++){
//            Double value = currentValues.get(i);
//            DataPoint dataPoint1 = new DataPoint(currentMinTimestamps.get(i), value);
//            DataPoint dataPoint2 = new DataPoint(currentMaxTimestamps.get(i), value);
//            statsAggregator.accept(dataPoint1, currentMeasures.get(i));
//            statsAggregator.accept(dataPoint2, currentMeasures.get(i));
//        }
//        LOG.debug("Created aggregate Datapoint {} - {} with measures {} with firsts: {}, lasts: {}, mins: {} and maxs: {} ",
//                DateTimeUtil.format(firstTimestamp), DateTimeUtil.format(lastTimestamp), statsAggregator.getMeasures(),
//                statsAggregator.getMeasures().stream().map(statsAggregator::getFirstDataPoint).collect(Collectors.toList()),
//                statsAggregator.getMeasures().stream().map(statsAggregator::getLastDataPoint).collect(Collectors.toList()),
//                statsAggregator.getMeasures().stream().map(statsAggregator::getMinValue).collect(Collectors.toList()),
//                statsAggregator.getMeasures().stream().map(statsAggregator::getMaxValue).collect(Collectors.toList()));
//        return new ImmutableAggregatedDataPoint(firstTimestamp, lastTimestamp, statsAggregator);
        return null;
    }
}

