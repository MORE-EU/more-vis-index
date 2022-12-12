package eu.more2020.visual.index.csv;

import eu.more2020.visual.domain.DataPoints;
import eu.more2020.visual.domain.csv.CsvAggregatedDataPoint;
import eu.more2020.visual.domain.csv.CsvDataPoint;
import eu.more2020.visual.index.TimeAggregator;
import eu.more2020.visual.util.DateTimeUtil;

import java.time.temporal.ChronoUnit;
import java.util.Arrays;

public class CsvTimeAggregator extends TimeAggregator implements CsvAggregatedDataPoint {

    private long fileOffset = -1;

    public CsvTimeAggregator(DataPoints sourceDataPoints, int interval, ChronoUnit unit) {
        super(sourceDataPoints, interval, unit);
    }

    @Override
    public long getFileOffset() {
        return fileOffset;
    }

    @Override
    protected void moveToNextInterval() {
        super.moveToNextInterval();
        fileOffset = ((CsvDataPoint) nextDataPoint).getFileOffset();
    }

    @Override
    public String toString() {
        return "{timestamp=" + DateTimeUtil.format(getTimestamp()) + ", offset=" + getFileOffset() + ", count=" + getCount() + ", stats=" + getStats() + '}';
    }
}
