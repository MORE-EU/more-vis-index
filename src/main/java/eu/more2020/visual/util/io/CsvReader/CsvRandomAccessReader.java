package eu.more2020.visual.util.io.CsvReader;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import eu.more2020.visual.util.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

public class CsvRandomAccessReader extends RandomAccessReader {
    private static final Logger LOG = LoggerFactory.getLogger(CsvRandomAccessReader.class);

    private final String filePath;
    private final CsvParserSettings csvParserSettings;
    private final DateTimeFormatter formatter;
    private final Integer timeCol;
    private final String delimiter;

    private final ZoneId zoneId = ZoneId.of("UTC");

    // the offset in the file of the first row (after the header, if there is one)
    private long startOffset;

    private long startTime;
    private long endTime;
    private CsvParser parser;

    private String[] parsedHeader;
    private List<Integer> measures;

    private Duration samplingInterval;

    private long meanByteSize;


    public CsvRandomAccessReader(String filePath,
                                 DateTimeFormatter formatter,
                                 Integer timeCol, String delimiter, Boolean hasHeader,
                                 List<Integer> measures) throws IOException {
        this(filePath, formatter, timeCol, delimiter, hasHeader, measures, Charset.defaultCharset());
    }

    public CsvRandomAccessReader(String filePath,
                                 DateTimeFormatter formatter,
                                 Integer timeCol, String delimiter, Boolean hasHeader,
                                 List<Integer> measures,
                                 Charset charset) throws IOException {
        this(filePath, formatter, timeCol, delimiter, hasHeader, measures, Charset.defaultCharset(), null, -1);
    }


    public CsvRandomAccessReader(String filePath,
                                 DateTimeFormatter formatter,
                                 Integer timeCol, String delimiter, Boolean hasHeader,
                                 List<Integer> measures,
                                 Charset charset, Duration samplingInterval, long meanByteSize) throws IOException {
        super(new File(filePath), charset);
        this.filePath = filePath;
        this.formatter = formatter;
        this.timeCol = timeCol;
        this.delimiter = delimiter;
        this.measures = measures;
        this.csvParserSettings = this.createCsvParserSettings();
        this.parser = new CsvParser(csvParserSettings);

        if (hasHeader) {
            // we first parse header, before selecting specific columns for the parser to process
            parsedHeader = parseLine(this.readNewLine());
        }

        updateMeasures(measures);

        // set start offset to offset of first data point
        startOffset = current;
        startTime = parseStringToTimestamp(parser.parseLine(this.readNewLine())[timeCol]);

        if (meanByteSize <= 0 || samplingInterval == null) {
            computeFileStats();
        } else {
            this.meanByteSize = meanByteSize;
            this.samplingInterval = samplingInterval;
        }

        // Reset file pointer to first data point
        this.setDirection(DIRECTION.FORWARD);
        this.seek(startOffset);

        LOG.info("Created reader for file {} with size {} bytes.", filePath, this.length());
        LOG.info("Start timestamp for file {} is {} at offset {}.", filePath, DateTimeUtil.format(startTime), startOffset);

    }


    private void computeFileStats() throws IOException {
        long secondTime = parseStringToTimestamp(parser.parseLine(this.readNewLine())[timeCol]);
        samplingInterval = Duration.of(secondTime - startTime, ChronoUnit.MILLIS);
        LOG.info("Sampling interval for file {}: {}.", filePath, samplingInterval);

        goToEnd();

        String lastLine = this.readNewLine();
        this.endTime = parseStringToTimestamp(parser.parseLine(lastLine)[timeCol]);
        LOG.info("End time for file {}: {}", filePath, endTime);

        long estimatedSamples = Duration.of(endTime - startTime, ChronoUnit.MILLIS).dividedBy(samplingInterval);
        LOG.info("Number of estimated rows in file {}: {}", filePath, estimatedSamples);
        meanByteSize = (this.length() - startOffset) / estimatedSamples;
        LOG.info("Mean row byte size in file {}: {}", filePath, meanByteSize);


/*        int sample_count = 1000000;
        long diff_sum = 0;
        long curr_offset = this.getFilePointer() - 1;
        int i = 0;
        while (i < sample_count && !this.isEOF()) {
            this.readNewLine();
            long offset = this.getFilePointer() - 1;
            diff_sum += offset - curr_offset;
            curr_offset = offset;
            i += 1;
        }

        this.meanByteSize = diff_sum / sample_count;*/
    }

    private long findPosition(long time) {
        return Duration.of(time - startTime, ChronoUnit.MILLIS).dividedBy(samplingInterval);
    }

    private long findProbabilisticOffset(long position) throws IOException {
        // todo why do we need to compare with the second part?
        return Math.min(startOffset + meanByteSize * position, (this.channel.size() - 100 * meanByteSize));
    }

    private long findOffset(long time) throws IOException {
        long position = findPosition(time);
        long probabilisticOffset = findProbabilisticOffset(position);

        this.setDirection(DIRECTION.FORWARD);
        this.seek(probabilisticOffset);

        // read a new line in case the probabilisticOffset does not match the start of a new line
        // todo: find a better way to handle this. Perhaps read the previous character to check if eol
        this.readNewLine();
        long prevOffset = this.getFilePointer();

        String line = this.readNewLine();

        long firstTimeFound = parseStringToTimestamp(this.parseLine(line)[timeCol]);

        long timeFound;
        if (firstTimeFound > time) {
            this.setDirection(DIRECTION.BACKWARD);
            while (true) {
                line = this.readNewLine();
                prevOffset = this.getFilePointer();
                timeFound = parseStringToTimestamp(this.parseLine(line)[timeCol]);
                if (timeFound <= time) return prevOffset;
            }
        } else if (firstTimeFound == time) {
            return prevOffset - 1;
        } else {
            while (true) {
                this.setDirection(DIRECTION.FORWARD);
                prevOffset = this.getFilePointer();
                line = this.readNewLine();
                timeFound = parseStringToTimestamp(this.parseLine(line)[timeCol]);
                if (timeFound >= time) return prevOffset - 1;
            }
        }
    }

    public void seekTimestamp(long timestamp) throws IOException {
        long offset = findOffset(timestamp);
        seek(offset);
    }

    public void goToEnd() throws IOException {
        this.seek(this.length() - 1);
        this.setDirection(DIRECTION.BACKWARD);
    }


    public String[] parseNext() throws IOException {
        String line;
        while ((line = readNewLine()) != null && line.isBlank());
        if (line == null)
            return null;
        return this.parser.parseLine(line);
    }

    private String[] parseLine(String line) {
        return this.parser.parseLine(line);
    }


    public String[] getParsedHeader() {
        return parsedHeader;
    }

    public Duration getSamplingInterval() {
        return samplingInterval;
    }

    public long getMeanByteSize() {
        return meanByteSize;
    }

    private void updateMeasures(List<Integer> measures) {
        this.measures = measures;
        List<Integer> tmpList = new ArrayList<>();
        tmpList.add(this.timeCol);
        tmpList.addAll(this.measures);
        this.csvParserSettings.selectIndexes(tmpList.toArray(new Integer[0]));
        this.parser = new CsvParser(this.csvParserSettings);
    }

    private long parseStringToTimestamp(String s) {
        ZonedDateTime zonedDateTime = LocalDateTime.parse(s, formatter).atZone(zoneId);
        return zonedDateTime.toInstant().toEpochMilli();
    }

/*    public ArrayList<String[]> getData(TimeRange range, List<Integer> measures) throws IOException {
        if (!measures.equals(this.measures)) updateMeasures(measures);
        ArrayList<String[]> rows = new ArrayList<>();
        long toOffset = findOffset(range.getTo());
        long fromOffset = findOffset(range.getFrom());
        this.seek(fromOffset);
        this.setDirection(DIRECTION.FORWARD);
        String line;
        while (this.getFilePointer() <= toOffset) {
            line = this.readNewLine();
            if (!line.isEmpty()) rows.add(parseLine(line));
        }
        return rows;
    }*/

    /*public String[] getData(long time, List<Integer> measures) throws IOException {
        if (!measures.equals(this.measures)) updateMeasures(measures);
        long position = findPosition(time);
        long probabilisticOffset = findProbabilisticOffset(position);

        this.setDirection(DIRECTION.FORWARD);
        this.seek(probabilisticOffset);

        String line = this.readNewLine();
        long firstTimeFound = parseStringToTimestamp(parseLine(line)[timeCol]);
        long timeFound;
        if (firstTimeFound > time) {
            this.setDirection(DIRECTION.BACKWARD);
            do {
                line = this.readNewLine();
                timeFound = parseStringToTimestamp(this.parseLine(line)[timeCol]);
            } while (!(timeFound <= time));
        } else {
            this.setDirection(DIRECTION.FORWARD);
            do {
                line = this.readNewLine();
                timeFound = parseStringToTimestamp(this.parseLine(line)[timeCol]);
            } while (!(timeFound >= (time)));
        }
        return parseLine(line);
    }*/

    private CsvParserSettings createCsvParserSettings() {
        CsvParserSettings parserSettings = new CsvParserSettings();
        parserSettings.getFormat().setDelimiter(this.delimiter.charAt(0));
        parserSettings.setIgnoreLeadingWhitespaces(false);
        parserSettings.setIgnoreTrailingWhitespaces(false);
        parserSettings.setLineSeparatorDetectionEnabled(true);
        parserSettings.setHeaderExtractionEnabled(false);
        // needed in order to be able to access measure values by col index
        parserSettings.setColumnReorderingEnabled(false);
        return parserSettings;
    }

}
