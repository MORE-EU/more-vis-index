package eu.more2020.visual.datasource;

import com.google.common.collect.Iterators;
import eu.more2020.visual.domain.*;
import eu.more2020.visual.domain.Dataset.ParquetDataset;
import eu.more2020.visual.domain.parquet.ParquetDataPoint;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;

public class ParquetDataSource implements DataSource {

    private final ParquetDataset parquetDataset;

    public ParquetDataSource(ParquetDataset parquetDataset) {
        (this).parquetDataset = parquetDataset;
    }

    @Override
    public DataPoints getDataPoints(long from, long to, List<Integer> measures) {
        return new ParquetDataSource.ParquetDataPoints(from, to, measures);
    }

    @Override
    public DataPoints getAllDataPoints(List<Integer> measures) {
        return new ParquetDataSource.ParquetDataPoints(parquetDataset.getTimeRange().getFrom(), parquetDataset.getTimeRange().getTo(), measures);
    }

    @Override
    public AggregatedDataPoints getAggregatedDataPoints(long from, long to, List<TimeRange> ranges,
                                                        List<Integer> measures, AggregateInterval aggregateInterval) {
        return null;
    }
    /**
     * Represents a series of {@link ParquetDataPoint} instances.
     * The iterator returned from this class accesses the Parquet files as the data points are requested.
     */
    final class ParquetDataPoints implements DataPoints {

        private final List<Integer> measures;

        private final long from;

        private final long to;


        public ParquetDataPoints(long from, long to, List<Integer> measures) {
            this.from = from;
            this.to = to;
            this.measures = measures;
        }

        @NotNull
        public Iterator<DataPoint> iterator() {
            ParquetDataPointsIterator[] iterators = parquetDataset.getFileInfoList().stream()
                    .filter(dataFileInfo -> dataFileInfo.getTimeRange().overlaps(this))
                    .map(dataFileInfo -> new ParquetDataPointsIterator(parquetDataset, dataFileInfo.getFilePath(), from, to, measures))
                    .toArray(ParquetDataPointsIterator[]::new);
            return Iterators.concat(iterators);
        }

        @Override
        public List<Integer> getMeasures() {
            return measures;
        }

        @Override
        public String toString() {
            return "ParquetDataPoints{" +
                    "measures=" + measures +
                    ", from=" + from +
                    ", to=" + to +
                    '}';
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
