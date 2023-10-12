package eu.more2020.visual.index.datasource;

import eu.more2020.visual.index.domain.Dataset.CsvDataset;
import eu.more2020.visual.index.domain.csv.CsvDataPoint;
import eu.more2020.visual.index.util.DateTimeUtil;
import eu.more2020.visual.index.util.io.CsvReader.CsvRandomAccessReader;
import eu.more2020.visual.index.util.io.CsvReader.DIRECTION;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class CsvDataPointsIterator implements Iterator<CsvDataPoint> {

    private final CsvDataset dataset;

    private final String filePath;

    private final long from;

    private final long to;
    private final List<Integer> measures;

    private CsvRandomAccessReader reader;
    private boolean started = false;
    private CsvDataPoint next;


    public CsvDataPointsIterator(CsvDataset dataset, String filePath, long from, long to, List<Integer> measures) {
        this.dataset = dataset;
        this.filePath = filePath;
        this.from = from;
        this.to = to;
        this.measures = measures;
    }

    // TODO: Handle null values in columns.
    private CsvDataPoint nextResult() {
        try {
            String[] row = reader.parseNext();
            if (row == null) {
                return null;
            }
            double[] values = new double[measures.size()];
            for (int i = 0; i < measures.size(); i++) {
                try {
                    values[i] = Double.parseDouble(row[measures.get(i)]);
                }
                catch (Exception e){ if(hasNext()) return  nextResult();}
            }
            return new CsvDataPoint(DateTimeUtil.parseDateTimeString(row[dataset.getTimeColIndex()], DateTimeFormatter.ofPattern(dataset.getTimeFormat()),
                    ZoneId.of("UTC")), values, reader.getLastRowReadOffset());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean hasNext() {
        if((to - from) < dataset.getSamplingInterval().toMillis()) return false;
        if (started) {
            return next != null && next.getTimestamp() < to;
        } else {
            try {
                started = true;
                reader = new CsvRandomAccessReader(filePath, DateTimeFormatter.ofPattern(dataset.getTimeFormat()), dataset.getTimeCol(),
                        dataset.getMeasures(), dataset.getDelimiter(),
                        dataset.getHasHeader(), Charset.defaultCharset(), dataset.getSamplingInterval(), dataset.getMeanByteSize());
                reader.seekTimestamp(from);
                reader.setDirection(DIRECTION.FORWARD);
                next = nextResult();
                return next != null && next.getTimestamp() <= to;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public CsvDataPoint next() {
        if (hasNext()) {
            CsvDataPoint current = next;
            next = nextResult();
            return current;
        }
        throw new NoSuchElementException();
    }
}