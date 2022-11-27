package eu.more2020.visual.index;

import eu.more2020.visual.datasource.DataSource;
import eu.more2020.visual.datasource.DataSourceFactory;
import eu.more2020.visual.domain.Dataset.AbstractDataset;
import eu.more2020.visual.domain.Query;
import eu.more2020.visual.domain.QueryResults;

import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;

public class TTI {

    private final AbstractDataset dataset;

    private final DataSource dataSource;


    // The top level view of the time series, with the coarser granularity.
    // This view covers the whole time series.
    private TimeSeriesSpan rootSpan;


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
        rootSpan = new TimeSeriesSpan();
        List<Integer> measures = query == null || query.getMeasures() == null ? dataset.getMeasures() : query.getMeasures();
        rootSpan.build(dataSource.getAllDataPoints(measures), 1, ChronoUnit.MINUTES, ZoneId.of("UTC"));
    }

    public QueryResults executeQuery(Query query) {
        QueryResults queryResults = new QueryResults();
        rootSpan.iterator(query.getRange().getFrom(), query.getRange().getTo(), query.getAggregator())
                .forEachRemaining(dataPoint -> {
                    System.out.println(dataPoint);
                });
        return queryResults;
    }



}
