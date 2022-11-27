package eu.more2020.visual.datasource;

import eu.more2020.visual.domain.Dataset.CsvDataset;
import eu.more2020.visual.domain.TimeRange;
import eu.more2020.visual.domain.csv.CsvDataPoint;
import eu.more2020.visual.util.DateTimeUtil;
import eu.more2020.visual.util.io.CsvReader.CsvRandomAccessReader;
import eu.more2020.visual.util.io.CsvReader.DIRECTION;

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
    private final TimeRange timeRange;
    private final List<Integer> measures;

    private CsvRandomAccessReader reader;
    private boolean started = false;
    private CsvDataPoint next;


    public CsvDataPointsIterator(CsvDataset dataset, String filePath, TimeRange timeRange, List<Integer> measures) {
        this.dataset = dataset;
        this.filePath = filePath;
        this.timeRange = timeRange;
        this.measures = measures;
    }


    private CsvDataPoint nextResult() {
        try {
            String[] row = reader.parseNext();
            if (row == null) {
                return null;
            }
            double[] values = new double[measures.size()];
            for (int i = 0; i < measures.size(); i++) {
                values[i] = Double.parseDouble(row[measures.get(i)]);
            }
            return new CsvDataPoint(DateTimeUtil.parseDateTimeString(row[dataset.getTimeCol()], DateTimeFormatter.ofPattern(dataset.getTimeFormat()),
                    ZoneId.of("UTC")), values, reader.getLastRowReadOffset());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean hasNext() {
        if (started) {
            return next != null && next.getTimestamp() <= timeRange.getTo();
        } else {
            try {
                started = true;
                reader = new CsvRandomAccessReader(filePath, DateTimeFormatter.ofPattern(dataset.getTimeFormat()), dataset.getTimeCol(), dataset.getDelimiter(),
                        dataset.getHasHeader(), measures, Charset.defaultCharset(), dataset.getSamplingInterval(), dataset.getMeanByteSize()) ;
                reader.seekTimestamp(timeRange.getFrom());
                reader.setDirection(DIRECTION.FORWARD);
                next = nextResult();
                return next != null && next.getTimestamp() <= timeRange.getTo();
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