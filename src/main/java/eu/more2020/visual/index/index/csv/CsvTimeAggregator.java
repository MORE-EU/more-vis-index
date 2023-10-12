package eu.more2020.visual.index.index.csv;

import eu.more2020.visual.index.domain.AggregateInterval;
import eu.more2020.visual.index.domain.DataPoint;
import eu.more2020.visual.index.domain.csv.CsvAggregatedDataPoint;
import eu.more2020.visual.index.index.MultiSpanIterator;
import eu.more2020.visual.index.index.TimeAggregator;

import java.util.List;

public class CsvTimeAggregator extends TimeAggregator implements CsvAggregatedDataPoint {
    /**
     * Constructs a {@link TimeAggregator}
     *
     * @param sourceDataPointsIterator
     * @param measures
     * @param aggInterval              The aggregation interval.
     */
    public CsvTimeAggregator(MultiSpanIterator<DataPoint> sourceDataPointsIterator, List<Integer> measures, AggregateInterval aggInterval) {
        super(sourceDataPointsIterator, measures, aggInterval);
    }

    @Override
    public long getFileOffset() {
        return 0;
    }
//
//    private long fileOffset = -1;
//
//    public CsvTimeAggregator(DataPoints sourceDataPoints, AggregateInterval aggregateInterval) {
//        super(sourceDataPoints, aggregateInterval);
//    }
//
//    @Override
//    public long getFileOffset() {
//        return fileOffset;
//    }
//
//    @Override
//    protected void moveToNextInterval() {
//        super.moveToNextInterval();
//        fileOffset = ((CsvDataPoint) nextDataPoint).getFileOffset();
//    }
//
//    @Override
//    public String toString() {
//        return "{timestamp=" + DateTimeUtil.format(getTimestamp()) + ", offset=" + getFileOffset() + ", count=" + getCount() + ", stats=" + getStats() + '}';
    }
