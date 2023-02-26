package eu.more2020.visual.index;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import eu.more2020.visual.datasource.DataSource;
import eu.more2020.visual.datasource.DataSourceFactory;
import eu.more2020.visual.domain.*;
import eu.more2020.visual.domain.Dataset.AbstractDataset;
import eu.more2020.visual.domain.Query.Query;
import eu.more2020.visual.util.DateTimeUtil;
import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.jetty.server.RequestLog;

import java.sql.Time;
import java.time.Duration;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class TTI {

    private final AbstractDataset dataset;

    private final DataSource dataSource;

    private final float accuracy = 0.9f;

    private boolean initialized = false;
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
                query.getTo(), query.getViewPort(), accuracy));

        Duration optimalM4Interval = DateTimeUtil.optimalM4(query.getFrom(), query.getTo(), query.getViewPort());
        timeSeriesSpan.build(dataPoints, accurateAggInterval, ZoneId.of("UTC"));
        intervalTree.insert(timeSeriesSpan);
        initialized = true;
    }

    Comparator<UnivariateDataPoint> compareLists = new Comparator<UnivariateDataPoint>() {
        @Override
        public int compare(UnivariateDataPoint s1, UnivariateDataPoint s2) {
            if (s1==null && s2==null) return 0;//swapping has no point here
            if (s1==null) return  1;
            if (s2==null) return -1;
            return (int) (s1.getTimestamp() - s2.getTimestamp());
        }
    };

    @SuppressWarnings("UnstableApiUsage")
    public QueryResults executeQuery(Query query) {
        if(!initialized) initialize(query);
        System.out.println(query);
        Duration optimalM4Interval = DateTimeUtil.optimalM4(query.getFrom(), query.getTo(), query.getViewPort());
        AggregateInterval optimalM4AggInterval = DateTimeUtil.aggregateCalendarInterval(optimalM4Interval);
        Duration accurateInterval = DateTimeUtil.accurateCalendarInterval(query.getFrom(), query.getTo(), query.getViewPort(), accuracy);
        AggregateInterval accurateAggInterval = DateTimeUtil.aggregateCalendarInterval(accurateInterval);
        List<Integer> measures = query.getMeasures() == null ? dataset.getMeasures() : query.getMeasures();

        QueryResults queryResults = new QueryResults();

        RangeSet<Long> rangeSet = TreeRangeSet.create();
        final ImmutableRangeSet<Long>[] currentDifference = new ImmutableRangeSet[]{ImmutableRangeSet.of(Range.closed(query.getFrom(), query.getTo()))};
        // Sort overlapping spans, by their query coverage. Then find which are the ones covering the whole range, and
        // also keep the remaining difference.
        List<TimeSeriesSpan> overlappingIntervals = StreamSupport.stream(Spliterators.spliteratorUnknownSize(intervalTree.overlappers(query), 0), false)
                .filter(span -> span.getAggregateInterval().toDuration()
                        .compareTo(optimalM4Interval) <= 0 && (span.overlaps(query)))
//                .sorted(Comparator.comparingDouble(span -> span.getAggregateInterval().toDuration().toMillis()))
                .sorted(Comparator.comparing(span -> span.percentage(query), Comparator.reverseOrder()))
                .filter(span -> {
                    if(currentDifference[0].isEmpty()) return false; // If the difference has been covered, don't check.
                    rangeSet.add(Range.closed(span.getFrom(), span.getTo()));
                    ImmutableRangeSet<Long> newDifference = currentDifference[0].difference(rangeSet);
                    if (!currentDifference[0].equals(newDifference)) { // If the current span, added to the difference, keep it.
                        currentDifference[0] = newDifference;
                        return true;
                    }
                    return false;
                })
                .collect(Collectors.toList());
        // Calculate and add missing intervals
        overlappingIntervals.addAll(currentDifference[0].asRanges().stream()
                .map(diff -> {
                    DataPoints dataPoints = dataSource.getDataPoints(diff.lowerEndpoint(), diff.upperEndpoint(), measures);
                    TimeSeriesSpan span = new TimeSeriesSpan();
                    span.build(dataPoints, accurateAggInterval, ZoneId.of("UTC"));
                    intervalTree.insert(span);
                    return span;
                }).collect(Collectors.toList()));

        MultiSpanIterator multiSpanIterator = new MultiSpanIterator(overlappingIntervals.iterator());
        PixelAggregator pixelAggregator = new PixelAggregator(multiSpanIterator, measures, optimalM4AggInterval);

        Map<Integer, List<UnivariateDataPoint>> data = measures.stream()
                .collect(Collectors.toMap(Function.identity(), ArrayList::new));

        while(pixelAggregator.hasNext()){
            AggregatedDataPoint next = pixelAggregator.next();
            Stats stats = next.getStats();
            if (stats.getCount() != 0) {
                for (int measure : measures) {
                    List<UnivariateDataPoint> measureData = data.get(measure);
                    measureData.add(new UnivariateDataPoint(stats.getMinTimestamp(measure), stats.getMinValue(measure)));
                    measureData.add(new UnivariateDataPoint(stats.getMaxTimestamp(measure), stats.getMaxValue(measure)));
                }
            }
        }
        data.forEach((k, v) -> v.sort(compareLists));
        queryResults.setData(data);
        return queryResults;
    }

}
