package eu.more2020.visual.index.domain.parquet;

import eu.more2020.visual.index.domain.AggregatedDataPoint;

public interface ParquetAggregatedDataPoint extends AggregatedDataPoint {

    /**
     * @return the file offset in the CSV file for the first raw datapoint of the window interval
     * represented by this instance
     */
    ParquetDataPoint.ParquetFileOffset getFileOffset();

}
