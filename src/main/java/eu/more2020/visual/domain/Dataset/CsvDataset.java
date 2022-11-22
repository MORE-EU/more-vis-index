package eu.more2020.visual.domain.Dataset;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import eu.more2020.visual.domain.DataFileInfo;
import eu.more2020.visual.domain.TimeRange;
import eu.more2020.visual.util.io.CsvReader.CsvRandomAccessReader;
import eu.more2020.visual.util.io.CsvReader.DIRECTION;
import org.apache.commons.io.input.ReversedLinesFileReader;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.stream.Collectors;

/**
 * A Dataset.
 */
public class CsvDataset extends AbstractDataset {

    public String delimiter;
    private Boolean hasHeader;

    public CsvDataset(String path, String id, String name, Integer timeCol,
                      List<Integer> measures, boolean hasHeader, String timeFormat, String delimiter) throws IOException {
        super(path, id, name, timeCol, measures, timeFormat);
        this.hasHeader = hasHeader;
        this.delimiter =  delimiter;
        this.fillCsvDatasetInfo();
    }

    private void fillCsvDataFileInfo(DataFileInfo dataFileInfo) throws IOException {
        CsvRandomAccessReader csvRandomAccessReader = new CsvRandomAccessReader(dataFileInfo.getFilePath(),
            new DateTimeFormatterBuilder().appendPattern(this.getTimeFormat())
            .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
            .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
            .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
            .toFormatter(), getTimeCol(), getDelimiter(), getHasHeader(), getMeasures());
        this.setHeader(getHasHeader() ? csvRandomAccessReader.parseHeader() : new String[0]);
        LocalDateTime from = csvRandomAccessReader.parseStringToDate(csvRandomAccessReader.parseNext()[this.getTimeCol()]);
        csvRandomAccessReader.goToEnd();
        LocalDateTime to = csvRandomAccessReader.parseStringToDate(csvRandomAccessReader.parseNext()[this.getTimeCol()]);
        dataFileInfo.setTimeRange(new TimeRange(from, to));
    }

    private void fillCsvDatasetInfo() throws IOException {
        File file = new File(this.getPath());
        if (!file.isDirectory()) {
            DataFileInfo dataFileInfo = new DataFileInfo(file.getAbsolutePath());
            fillCsvDataFileInfo(dataFileInfo);
            getFileInfoList().add(dataFileInfo);

        } else {
            List<DataFileInfo> fileInfoList = Arrays.stream(Objects.requireNonNull(file.listFiles(f -> !f.isDirectory() && f.getName().endsWith(".csv")))).map(f -> {
                DataFileInfo dataFileInfo = new DataFileInfo(f.getAbsolutePath());
                try {
                    fillCsvDataFileInfo(dataFileInfo);
                } catch (IOException e) {
                    new RuntimeException(e);
                }
                return dataFileInfo;
            }).sorted(Comparator.comparing(i -> i.getTimeRange().getFrom())).collect(Collectors.toList());
            // sort csv files by their time ranges ascending
            this.setFileInfoList(fileInfoList);
        }
    }

    public Boolean getHasHeader() {
        return hasHeader;
    }

    public void setHasHeader(Boolean hasHeader) {
        this.hasHeader = hasHeader;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public CsvParserSettings createCsvParserSettings() {
        CsvParserSettings parserSettings = new CsvParserSettings();
        parserSettings.getFormat().setDelimiter(this.getDelimiter().charAt(0));
        parserSettings.setIgnoreLeadingWhitespaces(false);
        parserSettings.setIgnoreTrailingWhitespaces(false);
        parserSettings.setLineSeparatorDetectionEnabled(true);
        parserSettings.setHeaderExtractionEnabled(false);
        return parserSettings;
    }


}
