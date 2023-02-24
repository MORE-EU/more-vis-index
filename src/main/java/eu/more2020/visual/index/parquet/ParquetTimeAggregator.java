package eu.more2020.visual.index.parquet;

import eu.more2020.visual.domain.AggregateInterval;
import eu.more2020.visual.domain.DataPoints;
import eu.more2020.visual.domain.parquet.ParquetAggregatedDataPoint;
import eu.more2020.visual.domain.parquet.ParquetDataPoint;
import eu.more2020.visual.index.TimeAggregator;
import eu.more2020.visual.util.DateTimeUtil;

public class ParquetTimeAggregator extends TimeAggregator implements ParquetAggregatedDataPoint {

    private ParquetDataPoint.ParquetFileOffset fileOffset = null;

    public ParquetTimeAggregator(DataPoints sourceDataPoints, AggregateInterval aggregateInterval) {
        super(sourceDataPoints, aggregateInterval);
    }

    @Override
    public ParquetDataPoint.ParquetFileOffset getFileOffset() {
        return fileOffset;
    }

    @Override
    protected void moveToNextInterval() {
        super.moveToNextInterval();
        fileOffset = ((ParquetDataPoint) nextDataPoint).getParquetFileOffset();
    }

    @Override
    public String toString() {
        return "{timestamp=" + DateTimeUtil.format(getTimestamp()) + ", offset=" + getFileOffset() + ", count=" + getCount() + ", stats=" + getStats() + '}';
    }
}