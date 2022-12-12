package eu.more2020.visual.index;

import eu.more2020.visual.datasource.DataSource;
import eu.more2020.visual.datasource.DataSourceFactory;
import eu.more2020.visual.domain.DataPoints;
import eu.more2020.visual.domain.Dataset.AbstractDataset;
import eu.more2020.visual.domain.Query;
import eu.more2020.visual.domain.QueryResults;
import eu.more2020.visual.util.DateTimeUtil;

import java.time.Duration;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

public class TTI {

    private final AbstractDataset dataset;

    private final DataSource dataSource;


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

        Duration optimalM4Interval  = DateTimeUtil.optimalM4(query.getFrom(), query.getTo(), query.getViewPort());
        Duration aggInterval = DateTimeUtil.maxCalendarInterval(optimalM4Interval);
        timeSeriesSpan.build(dataPoints, 1, ChronoUnit.MINUTES, ZoneId.of("UTC"));
        intervalTree.insert(timeSeriesSpan);
    }

    public QueryResults executeQuery(Query query) {
        Duration optimalM4Interval  = DateTimeUtil.optimalM4(query.getFrom(), query.getTo(), query.getViewPort());
        Duration aggInterval = DateTimeUtil.maxCalendarInterval(optimalM4Interval);

        List<Integer> measures = query.getMeasures() == null ? dataset.getMeasures() : query.getMeasures();

        QueryResults queryResults = new QueryResults();
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(intervalTree.overlappers(query), 0), false)
                .filter(span -> Duration.of(span.getInterval(), span.getUnit())
                        .compareTo(aggInterval) <= 0 && span.encloses(query))
                .sorted(Comparator.comparingDouble(TimeSeriesSpan::getInterval)).findFirst()
                .ifPresentOrElse(timeSeriesSpan -> {
                }, () -> {
                    DataPoints dataPoints = dataSource.getDataPoints(query.getFrom(), query.getTo(), measures);
                    TimeSeriesSpan timeSeriesSpan = new TimeSeriesSpan();
                    timeSeriesSpan.build(dataPoints, 1, ChronoUnit.MINUTES, ZoneId.of("UTC"));
                    intervalTree.insert(timeSeriesSpan);
                });
        return queryResults;
    }


}
