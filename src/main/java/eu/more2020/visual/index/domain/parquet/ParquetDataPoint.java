package eu.more2020.visual.index.domain.parquet;

import eu.more2020.visual.index.domain.ImmutableDataPoint;
import eu.more2020.visual.index.util.DateTimeUtil;

import java.util.Arrays;

/**
 * Represents a single multi-measure data point mapped by a single row of a PARQUET file
 */
public class ParquetDataPoint extends ImmutableDataPoint {

    // The row group id of the datapoint in the file
    private final ParquetFileOffset parquetFileOffset;

    public ParquetDataPoint(long timestamp, double[] values, long rowGroupId, long rowGroupOffset) {
        super(timestamp, values);
        this.parquetFileOffset = new ParquetFileOffset(rowGroupId, rowGroupOffset);
    }

    public ParquetFileOffset getParquetFileOffset() {
        return parquetFileOffset;
    }

    @Override
    public String toString() {
        return "ParquetDataPoint{" +
                "timestamp=" + DateTimeUtil.format(getTimestamp())  +
                ", rowGroupId=" + parquetFileOffset.getRowGroupId() +
                ", rowGroupOffset=" + parquetFileOffset.getRowGroupOffset() +
                ", values=" + Arrays.toString(getValues()) +
                '}';
    }

    public static class ParquetFileOffset {

        protected final long rowGroupId;
        protected final long rowGroupOffset;

        public ParquetFileOffset(long rowGroupId, long rowGroupOffset) {
            this.rowGroupId = rowGroupId;
            this.rowGroupOffset = rowGroupOffset;
        }

        public long getRowGroupId() {
            return rowGroupId;
        }

        public long getRowGroupOffset() {
            return rowGroupOffset;
        }
    }
}
