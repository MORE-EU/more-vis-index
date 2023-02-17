package eu.more2020.visual.index.parquet;

import eu.more2020.visual.domain.AggregatedDataPoint;
import eu.more2020.visual.domain.csv.CsvAggregatedDataPoint;
import eu.more2020.visual.domain.parquet.ParquetDataPoint;
import eu.more2020.visual.index.TimeSeriesSpan;

import java.util.List;

public class ParquetTimeSeriesSpan extends TimeSeriesSpan {

    /**
     * The file offset in the CSV file for the first datapoint
     * included in every aggregate interval of this time series span
     */
    private ParquetDataPoint.ParquetFileOffset[] fileOffsets;

    @Override
    protected void addAggregatedDataPoint(int i, List<Integer> measures, AggregatedDataPoint aggregatedDataPoint) {
        super.addAggregatedDataPoint(i, measures, aggregatedDataPoint);
        fileOffsets[i] = ((ParquetDataPoint) aggregatedDataPoint).getParquetFileOffset();
    }
}
