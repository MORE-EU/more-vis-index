package eu.more2020.visual.datasource;

import com.google.common.collect.Iterators;
import eu.more2020.visual.domain.*;
import eu.more2020.visual.domain.Dataset.CsvDataset;
import eu.more2020.visual.domain.csv.CsvDataPoint;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;

public class CsvDataSource implements DataSource {

    private final CsvDataset csvDataset;

    public CsvDataSource(CsvDataset csvDataset) {
        this.csvDataset = csvDataset;
    }

    @Override
    public CsvDataPoints getDataPoints(long from, long to, List<Integer> measures) {
        return new CsvDataPoints(from, to, measures);
    }

    @Override
    public CsvDataPoints getAllDataPoints(List<Integer> measures) {
        return new CsvDataPoints(csvDataset.getTimeRange().getFrom(), csvDataset.getTimeRange().getTo(), measures);
    }

    @Override
    public AggregatedDataPoints getAggregatedDataPoints(long from, long to, List<TimeRange> ranges,
                                                        List<Integer> measures, AggregateInterval aggregateInterval) {
        return null;
    }

    /**
     * Represents a series of {@link CsvDataPoint} instances.
     * The iterator returned from this class accesses the CSV files as the data points are requested.
     */
    final class CsvDataPoints implements DataPoints {

        private final List<Integer> measures;

        private final long from;

        private final long to;


        public CsvDataPoints(long from, long to, List<Integer> measures) {
            this.from = from;
            this.to = to;
            this.measures = measures;
        }

        @NotNull
        public Iterator<DataPoint> iterator() {
            CsvDataPointsIterator[] iterators = csvDataset.getFileInfoList().stream()
                    .filter(dataFileInfo -> dataFileInfo.getTimeRange().overlaps(this))
                    .map(dataFileInfo -> new CsvDataPointsIterator(csvDataset, dataFileInfo.getFilePath(), from, to, measures))
                    .toArray(CsvDataPointsIterator[]::new);
            return Iterators.concat(iterators);
        }

        @Override
        public List<Integer> getMeasures() {
            return measures;
        }

        @Override
        public long getFrom() {
            return from;
        }

        @Override
        public long getTo() {
            return to;
        }

        @Override
        public String getFromDate() {
            return getFromDate("yyyy-MM-dd HH:mm:ss");
        }

        @Override
        public String getToDate() {
            return getToDate("yyyy-MM-dd HH:mm:ss");
        }

        @Override
        public String getFromDate(String format) {
            return Instant.ofEpochMilli(getTo()).atZone(ZoneId.of("UTC"))
                    .format(DateTimeFormatter.ofPattern(format));
        }

        @Override
        public String getToDate(String format) {
            return Instant.ofEpochMilli(getTo()).atZone(ZoneId.of("UTC"))
                    .format(DateTimeFormatter.ofPattern(format));
        }


    }

}
