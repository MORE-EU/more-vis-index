package eu.more2020.visual.datasource;

import eu.more2020.visual.domain.Dataset.ParquetDataset;
import eu.more2020.visual.domain.TimeRange;
import eu.more2020.visual.domain.csv.CsvDataPoint;
import eu.more2020.visual.domain.parquet.ParquetDataPoint;
import eu.more2020.visual.util.DateTimeUtil;
import eu.more2020.visual.util.io.ParquetReader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class ParquetDataPointsIterator implements Iterator<ParquetDataPoint> {

    private final ParquetDataset dataset;

    private final String filePath;
    private final long from;
    private final long to;
    private final List<Integer> measures;

    private ParquetReader reader;
    private boolean started = false;
    private ParquetDataPoint next;

    public ParquetDataPointsIterator(ParquetDataset dataset, String filePath, long from, long to, List<Integer> measures) {
        this.dataset = dataset;
        this.filePath = filePath;
        this.from = from;
        this.to = to;
        this.measures = measures;
    }

    private ParquetDataPoint nextResult() {
        try {
            String[] row = reader.parseNext();
            if (row == null) {
                return null;
            }
            double[] values = new double[measures.size()];
            for (int i = 1; i < measures.size(); i++) {
                values[i] = Double.parseDouble(row[i]);
            }
            return new ParquetDataPoint(DateTimeUtil.parseDateTimeString(row[0], DateTimeFormatter.ofPattern(dataset.getTimeFormat()),
                    ZoneId.of("UTC")), values, 0, 0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }


    @Override
    public boolean hasNext() {
        if((to - from) < dataset.getSamplingInterval().toMillis()) return false;
        if (started) {
            try {
                return reader.hasNext() && next.getTimestamp() < to;
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            try {
                started = true;
                reader = new ParquetReader(filePath, DateTimeFormatter.ofPattern(dataset.getTimeFormat()), dataset.getTimeCol(),
                        measures, dataset.getSamplingInterval()) ;
                reader.seekTimestamp(from);
                next = nextResult();
                return reader.hasNext() && next.getTimestamp() <= to;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return false;
    }

    @Override
    public ParquetDataPoint next() {
        if (hasNext()) {
            ParquetDataPoint current = next;
            next = nextResult();
            return current;
        }
        throw new NoSuchElementException();
    }
}
