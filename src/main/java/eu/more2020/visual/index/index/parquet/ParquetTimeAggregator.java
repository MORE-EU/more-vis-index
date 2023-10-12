package eu.more2020.visual.index.index.parquet;

import eu.more2020.visual.index.domain.AggregateInterval;
import eu.more2020.visual.index.domain.DataPoint;
import eu.more2020.visual.index.domain.parquet.ParquetAggregatedDataPoint;
import eu.more2020.visual.index.domain.parquet.ParquetDataPoint;
import eu.more2020.visual.index.index.MultiSpanIterator;
import eu.more2020.visual.index.index.TimeAggregator;

import java.util.List;

public class ParquetTimeAggregator extends TimeAggregator implements ParquetAggregatedDataPoint {
    /**
     * Constructs a {@link TimeAggregator}
     *
     * @param sourceDataPointsIterator
     * @param measures
     * @param aggInterval              The aggregation interval.
     */
    public ParquetTimeAggregator(MultiSpanIterator<DataPoint> sourceDataPointsIterator, List<Integer> measures, AggregateInterval aggInterval) {
        super(sourceDataPointsIterator, measures, aggInterval);
    }

    @Override
    public ParquetDataPoint.ParquetFileOffset getFileOffset() {
        return null;
    }

//    private ParquetDataPoint.ParquetFileOffset fileOffset = null;
//
//    public ParquetTimeAggregator(DataPoints sourceDataPoints, AggregateInterval aggregateInterval) {
//        super(sourceDataPoints, aggregateInterval);
//    }
//
//    @Override
//    public ParquetDataPoint.ParquetFileOffset getFileOffset() {
//        return fileOffset;
//    }
//
//    @Override
//    protected void moveToNextInterval() {
//        super.moveToNextInterval();
//        fileOffset = ((ParquetDataPoint) nextDataPoint).getParquetFileOffset();
//    }
//
//    @Override
//    public String toString() {
//        return "{timestamp=" + DateTimeUtil.format(getTimestamp()) + ", offset=" + getFileOffset() + ", count=" + getCount() + ", stats=" + getStats() + '}';
//    }
}
