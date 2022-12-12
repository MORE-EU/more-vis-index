package eu.more2020.visual.domain.parquet;

import eu.more2020.visual.domain.ImmutableDataPoint;
import eu.more2020.visual.util.DateTimeUtil;

import java.util.Arrays;

/**
 * Represents a single multi-measure data point mapped by a single row of a PARQUET file
 */
public class ParquetDataPoint extends ImmutableDataPoint {

    // The row group id of the datapoint in the file
    private final long rowGroupId;
    private final long rowGroupOffset;

    public ParquetDataPoint(long timestamp, double[] values, long rowGroupId, long rowGroupOffset) {
        super(timestamp, values);
        this.rowGroupId = rowGroupId;
        this.rowGroupOffset = rowGroupOffset;
    }

    public long getRowGroupId() {
        return rowGroupId;
    }

    public long getRowGroupOffset() {
        return rowGroupOffset;
    }

    @Override
    public String toString() {
        return "ParquetDataPoint{" +
                "timestamp=" + DateTimeUtil.format(getTimestamp())  +
                ", rowGroupId=" + rowGroupId +
                ", rowGroupOffset=" + rowGroupOffset +
                ", values=" + Arrays.toString(getValues()) +
                '}';
    }
}
