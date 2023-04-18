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

import javax.management.openmbean.SimpleType;
import java.time.Duration;
import java.time.ZoneId;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class RawTTI {

    private final AbstractDataset dataset;

    private final DataSource dataSource;

    private boolean initialized = false;
    // The interval tree containing all the time series spans already cached
    private IntervalTree<RawTimeSeriesSpan> intervalTree;


    /**
     * Creates a new TTI for a multi measure-time series
     *
     * @param dataset
     */
    public RawTTI(AbstractDataset dataset) {
        this.dataset = dataset;
        this.dataSource = DataSourceFactory.getDataSource(dataset);
    }


    public void initialize(Query query) {
        intervalTree = new IntervalTree<>();
        List<Integer> measures = query == null || query.getMeasures() == null ? dataset.getMeasures() : query.getMeasures();
        RawTimeSeriesSpan timeSeriesSpan = new RawTimeSeriesSpan();
        DataPoints dataPoints = dataSource.getDataPoints(query.getFrom(), query.getTo(), measures);
        timeSeriesSpan.build(dataPoints);
        intervalTree.insert(timeSeriesSpan);
        initialized = true;
    }

    @SuppressWarnings("UnstableApiUsage")
    public QueryResults executeQuery(Query query) {
        if(!initialized) initialize(query);
        System.out.println(query);

        List<Integer> measures = query.getMeasures() == null ? dataset.getMeasures() : query.getMeasures();

        QueryResults queryResults = new QueryResults();
        final int[] ioCount = {0};

        RangeSet<Long> rangeSet = TreeRangeSet.create();
        final ImmutableRangeSet<Long>[] currentDifference = new ImmutableRangeSet[]{ImmutableRangeSet.of(Range.closed(query.getFrom(), query.getTo()))};
        // Sort overlapping spans, by their query coverage. Then find which are the ones covering the whole range, and
        // also keep the remaining difference.
        List<RawTimeSeriesSpan> overlappingIntervals = StreamSupport.stream(Spliterators.spliteratorUnknownSize(intervalTree.overlappers(query), 0), false)
                .filter(span ->  (span.overlaps(query)))
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
                    RawTimeSeriesSpan span = new RawTimeSeriesSpan();
                    span.build(dataPoints);
                    ioCount[0] += span.getCount();
                    System.out.println(span);
                    intervalTree.insert(span);
                    return span;
                }).collect(Collectors.toList()));

        MultiSpanIterator multiSpanIterator = new MultiSpanIterator(overlappingIntervals.iterator());

        Map<Integer, List<UnivariateDataPoint>> data = measures.stream()
                .collect(Collectors.toMap(Function.identity(), ArrayList::new));

        while(multiSpanIterator.hasNext()){
            DataPoint next = (DataPoint) multiSpanIterator.next();
            long timestamp = next.getTimestamp();
            double[] values = next.getValues();
            int i = 0;
            for (int m : measures) {
                List<UnivariateDataPoint> measureData = data.get(m);
                measureData.add(new UnivariateDataPoint(timestamp, values[i]));
                i++;
            }
        }
        queryResults.setIoCount(ioCount[0]);
        queryResults.setData(data);
        return queryResults;
    }

    /**
     * Calculates the deep memory size of this instance.
     *
     * @return The deep memory size in bytes.
     */
    public long calculateDeepMemorySize() {
        long size = 0L;
        for(RawTimeSeriesSpan span : intervalTree){
            size += span.calculateDeepMemorySize();
        }
        return size;
    }
}
