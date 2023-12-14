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
    private final List<TimeInterval> unionTimeIntervals;
    private final Map<Integer, Long> aggregateIntervals;


    public PostgreSQLAggregateDataPointsIteratorM4(ResultSet resultSet,
                                                   Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
                                                   Map<Integer, Long> aggregateIntervals) throws SQLException {
        this.resultSet = resultSet;
        this.unionTimeIntervals = missingIntervalsPerMeasure.values().stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());
        this.aggregateIntervals = aggregateIntervals;
    }

    @Override
    public boolean hasNext() {
        try {
            return resultSet.next();
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
        try {
            int measure = resultSet.getInt(1);
            long t_min = resultSet.getLong(2);
            long t_max = resultSet.getLong(3);
            double value = resultSet.getDouble(4);
            int k = resultSet.getInt(5);
            int unionGroup = resultSet.getInt(6); // signifies the union id
            Long aggregateInterval = aggregateIntervals.get(measure);

            StatsAggregator statsAggregator = new StatsAggregator();

            TimeInterval correspondingInterval = unionTimeIntervals.get(unionGroup);

            long firstTimestamp = correspondingInterval.getFrom() + k * aggregateInterval;
            long lastTimestamp = correspondingInterval.getFrom() + ((k + 1) * aggregateInterval);
            if(firstTimestamp + aggregateInterval > correspondingInterval.getTo()) {
                lastTimestamp = correspondingInterval.getTo();
            }
            DataPoint dataPoint1 = new ImmutableDataPoint(t_min, value);
            DataPoint dataPoint2 = new ImmutableDataPoint(t_max, value);
            statsAggregator.accept(dataPoint1);
            statsAggregator.accept(dataPoint2);
            LOG.debug("Created aggregate Datapoint {} - {} with firsts: {}, last: {}, min: {} and max: {} ",
                    DateTimeUtil.format(firstTimestamp), DateTimeUtil.format(lastTimestamp),
                    statsAggregator.getFirstValue(),
                    statsAggregator.getLastValue(),
                    statsAggregator.getMinValue(),
                    statsAggregator.getMaxValue());
            return new ImmutableAggregatedDataPoint(firstTimestamp, lastTimestamp, measure, statsAggregator);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}

