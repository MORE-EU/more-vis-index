package eu.more2020.visual.domain.Dataset;

import com.univocity.parsers.csv.CsvParserSettings;
import eu.more2020.visual.domain.DataFileInfo;
import eu.more2020.visual.domain.TimeRange;
import eu.more2020.visual.util.DateTimeUtil;
import eu.more2020.visual.util.io.CsvReader.CsvRandomAccessReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A Dataset.
 */
public class CsvDataset extends AbstractDataset {

    private static final Logger LOG = LoggerFactory.getLogger(CsvRandomAccessReader.class);

    public String delimiter;
    private Boolean hasHeader;

    private long meanByteSize;

    public CsvDataset(String path, String id, String name, String timeColName,
                      List<String> measureNames, boolean hasHeader, String timeFormat, String delimiter) throws IOException {
        super(path, id, name, timeColName, measureNames, timeFormat);
        this.hasHeader = hasHeader;
        this.delimiter = delimiter;
        this.fillCsvDatasetInfo();
        LOG.info("Initialized dataset: {}", this);
    }

    public CsvDataset(String path, String id, String name, Integer timeCol,
                      List<Integer> measures, boolean hasHeader, String timeFormat, String delimiter) throws IOException {
        super(path, id, name, timeCol, measures, timeFormat);
        this.hasHeader = hasHeader;
        this.delimiter = delimiter;
        this.fillCsvDatasetInfo();
        LOG.info("Initialized dataset: {}", this);
    }

    private void fillCsvDataFileInfo(DataFileInfo dataFileInfo) throws IOException {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(this.getTimeFormat());

        CsvRandomAccessReader csvRandomAccessReader = new CsvRandomAccessReader(dataFileInfo.getFilePath(), formatter,
                getTimeCol(), getDelimiter(), getHasHeader(), getMeasures());

        if (hasHeader) {
            setHeader(csvRandomAccessReader.getParsedHeader());
        }
        // todo: we don't need to update these with every sub-file
        setSamplingInterval(csvRandomAccessReader.getSamplingInterval());
        meanByteSize = csvRandomAccessReader.getMeanByteSize();

        long from = DateTimeUtil.parseDateTimeString(csvRandomAccessReader.parseNext()[this.getTimeCol()], formatter, ZoneId.of("UTC"));
        csvRandomAccessReader.goToEnd();
        long to = DateTimeUtil.parseDateTimeString(csvRandomAccessReader.parseNext()[this.getTimeCol()], formatter, ZoneId.of("UTC"));

        dataFileInfo.setTimeRange(new TimeRange(from, to));
    }

    private void fillCsvDatasetInfo() throws IOException {
        File file = new File(this.getPath());
        if (!file.isDirectory()) {
            DataFileInfo dataFileInfo = new DataFileInfo(file.getAbsolutePath());
            fillCsvDataFileInfo(dataFileInfo);
            getFileInfoList().add(dataFileInfo);
        } else {
            List<DataFileInfo> fileInfoList = Arrays.stream(Objects.requireNonNull(file.listFiles(f -> !f.isDirectory() && f.getName()
                    .endsWith(".csv")))).map(f -> {
                DataFileInfo dataFileInfo = new DataFileInfo(f.getAbsolutePath());
                try {
                    fillCsvDataFileInfo(dataFileInfo);
                } catch (IOException e) {
                    new UncheckedIOException(e);
                }
                return dataFileInfo;
            }).sorted(Comparator.comparing(i -> i.getTimeRange().getFrom())).collect(Collectors.toList());
            // sort csv files by their time ranges ascending
            this.setFileInfoList(fileInfoList);
        }
        // set time range for complete dataset from all files
        if (fileInfoList != null && !fileInfoList.isEmpty()) {
            this.setTimeRange(new TimeRange(fileInfoList.get(0).getTimeRange()
                    .getFrom(), fileInfoList.get(fileInfoList.size() - 1).getTimeRange().getTo()));
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

    public long getMeanByteSize() {
        return meanByteSize;
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
