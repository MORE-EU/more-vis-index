package eu.more2020.visual.index.domain.csv;

import eu.more2020.visual.index.domain.ImmutableDataPoint;
import eu.more2020.visual.index.util.DateTimeUtil;

import java.util.Arrays;

/**
 * Represents a single multi-measure data point mapped by a single row of a CSV file
 */
public class CsvDataPoint extends ImmutableDataPoint {

    // The datapoint's row offset in the CSV file
    private final long fileOffset;

    public CsvDataPoint(final long timestamp, final double[] values, long fileOffset) {
        super(timestamp, values);
        this.fileOffset = fileOffset;
    }


    /**
     * @return The datapoint's row offset in the CSV file
     */
    public long getFileOffset() {
        return fileOffset;
    }

    @Override
    public String toString() {
        return "CsvDataPoint{" +
                "timestamp=" + DateTimeUtil.format(getTimestamp())  +
                ", fileOffset=" + fileOffset +
                ", values=" + Arrays.toString(getValues()) +
                '}';
    }
}
