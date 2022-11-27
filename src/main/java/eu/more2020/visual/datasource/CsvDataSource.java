package eu.more2020.visual.datasource;

import com.google.common.collect.Iterators;
import eu.more2020.visual.domain.DataPoint;
import eu.more2020.visual.domain.DataPoints;
import eu.more2020.visual.domain.Dataset.CsvDataset;
import eu.more2020.visual.domain.TimeRange;
import eu.more2020.visual.domain.csv.CsvDataPoint;

import java.util.Iterator;
import java.util.List;

public class CsvDataSource implements DataSource {

    private final CsvDataset csvDataset;

    public CsvDataSource(CsvDataset csvDataset) {
        this.csvDataset = csvDataset;
    }

    @Override
    public CsvDataPoints getDataPoints(TimeRange timeRange, List<Integer> measures) {
        return new CsvDataPoints(timeRange, measures);
    }

    @Override
    public CsvDataPoints getAllDataPoints(List<Integer> measures) {
        return new CsvDataPoints(csvDataset.getTimeRange(), measures);
    }

    /**
     * Represents a series of {@link CsvDataPoint} instances.
     * The iterator returned from this class accesses the CSV files as the data points are requested.
     */
    final class CsvDataPoints implements DataPoints {

        private final List<Integer> measures;
        private final TimeRange timeRange;


        public CsvDataPoints(TimeRange timeRange, List<Integer> measures) {
            this.timeRange = timeRange;
            this.measures = measures;
        }

        public Iterator<DataPoint> iterator() {
            CsvDataPointsIterator[] iterators = csvDataset.getFileInfoList().stream()
                    .filter(dataFileInfo -> timeRange == null || dataFileInfo.getTimeRange().intersects(timeRange))
                    .map(dataFileInfo -> new CsvDataPointsIterator(csvDataset, dataFileInfo.getFilePath(), timeRange, measures))
                    .toArray(CsvDataPointsIterator[]::new);
            return Iterators.concat(iterators);
        }

        @Override
        public List<Integer> getMeasures() {
            return measures;
        }

        @Override
        public TimeRange getTimeRange() {
            return timeRange;
        }

        @Override
        public String toString() {
            return "CsvDataPoints{" +
                    "measures=" + measures +
                    ", timeRange=" + timeRange +
                    '}';
        }
    }

}
