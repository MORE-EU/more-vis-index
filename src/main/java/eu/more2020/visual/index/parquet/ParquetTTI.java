package eu.more2020.visual.index.parquet;

import eu.more2020.visual.domain.Dataset.ParquetDataset;
import eu.more2020.visual.domain.TimeRange;
import eu.more2020.visual.util.io.ParquetReader;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.*;

public class ParquetTTI {

    private ParquetDataset dataset;
    private String filePath;
    private int objectsIndexed = 0;
    private boolean isInitialized = false;
    private DateTimeFormatter formatter;
    private ParquetReader parquetReader;
    private Map<Integer, DoubleSummaryStatistics> measureStats;


    public ParquetTTI(String filePath, ParquetDataset dataset) {
        this.dataset = dataset;
        this.filePath = filePath;
        this.formatter =
            new DateTimeFormatterBuilder().appendPattern(dataset.getTimeFormat())
                .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
                .toFormatter();
    }

    public void initialize() throws IOException {
        this.objectsIndexed = 0;
        List<Integer> measures = dataset.getMeasures();
        measureStats = new HashMap<>();
        for (Integer measureIndex : dataset.getMeasures()) {
            measureStats.put(measureIndex, new DoubleSummaryStatistics());
        }
        this.parquetReader = new ParquetReader(filePath, dataset.getTimeCol(), measures, formatter);
        this.dataset.setSamplingInterval(this.parquetReader.sample());
        this.isInitialized = true;
    }

    public DateTimeFormatter getFormatter(){
        return this.formatter;
    }


    public ArrayList<String[]> testRandomAccessRange (TimeRange range, List<Integer> measures) throws IOException {
        return this.parquetReader.getData(range, measures);
    }

}
