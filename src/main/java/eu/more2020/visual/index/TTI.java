package eu.more2020.visual.index;

import eu.more2020.visual.datasource.DataSource;
import eu.more2020.visual.datasource.DataSourceFactory;
import eu.more2020.visual.domain.*;
import eu.more2020.visual.domain.Dataset.AbstractDataset;
import eu.more2020.visual.util.DateTimeUtil;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Comparator;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class TTI {

    private final AbstractDataset dataset;

    private final DataSource dataSource;

    private final float accuracy = 0.9f;


    // The interval tree containing all the time series spans already cached
    private IntervalTree<TimeSeriesSpan> intervalTree;


    /**
     * Creates a new TTI for a multi measure-time series
     *
     * @param dataset
     */
    public TTI(AbstractDataset dataset) {
        this.dataset = dataset;
        this.dataSource = DataSourceFactory.getDataSource(dataset);
    }


    public void initialize(Query query) {
        intervalTree = new IntervalTree<>();
        List<Integer> measures = query == null || query.getMeasures() == null ? dataset.getMeasures() : query.getMeasures();
        TimeSeriesSpan timeSeriesSpan = new TimeSeriesSpan();
        DataPoints dataPoints = dataSource.getDataPoints(query.getFrom(), query.getTo(), measures);

        AggregateInterval accurateAggInterval = DateTimeUtil.aggregateCalendarInterval(DateTimeUtil.accurateCalendarInterval(query.getFrom(),
                query.getTo(), query.getViewPort(), dataset.getSamplingInterval(), accuracy));

        Duration optimalM4Interval = DateTimeUtil.optimalM4(query.getFrom(), query.getTo(), query.getViewPort());
        Duration aggInterval = DateTimeUtil.maxCalendarInterval(optimalM4Interval);
        timeSeriesSpan.build(dataPoints, accurateAggInterval, ZoneId.of("UTC"));
        intervalTree.insert(timeSeriesSpan);
    }

    public QueryResults executeQuery(Query query) {
        Duration optimalM4Interval = DateTimeUtil.optimalM4(query.getFrom(), query.getTo(), query.getViewPort());
        Duration accurateInterval = DateTimeUtil.accurateCalendarInterval(query.getFrom(), query.getTo(), query.getViewPort(), dataset.getSamplingInterval(), accuracy);
        AggregateInterval accurateAggInterval = DateTimeUtil.aggregateCalendarInterval(accurateInterval);

        List<Integer> measures = query.getMeasures() == null ? dataset.getMeasures() : query.getMeasures();

        QueryResults queryResults = new QueryResults();
        TimeSeriesSpan timeSeriesSpan = StreamSupport.stream(Spliterators.spliteratorUnknownSize(intervalTree.overlappers(query), 0), false)
                .filter(span -> span.getAggregateInterval().toDuration()
                        .compareTo(optimalM4Interval) <= 0 && span.encloses(query))
                .sorted(Comparator.comparingDouble(span -> span.getAggregateInterval().toDuration().toMillis()))
                .findFirst()
                .orElseGet(() -> {
                    DataPoints dataPoints = dataSource.getDataPoints(query.getFrom(), query.getTo(), measures);
                    TimeSeriesSpan span = new TimeSeriesSpan();
                    span.build(dataPoints, accurateAggInterval, ZoneId.of("UTC"));
                    intervalTree.insert(span);
                    return span;
                });

        queryResults.setData(StreamSupport.stream(Spliterators.spliteratorUnknownSize(timeSeriesSpan.iterator(query.getFrom(), query.getTo(), Aggregator.AVG), 0), false)
                .collect(Collectors.toList()));
        return queryResults;
    }


}
