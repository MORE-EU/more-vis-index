package eu.more2020.visual.util.io.CsvReader;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import eu.more2020.visual.domain.TimeRange;
import eu.more2020.visual.index.csv.CsvQueryProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class CsvRandomAccessReader extends RandomAccessReader {
    private static final Logger LOG = LogManager.getLogger(CsvQueryProcessor.class);

    private final CsvParserSettings csvParserSettings;
    private final DateTimeFormatter formatter;
    private final String filePath;
    private final boolean hasHeader;
    private final Integer timeCol;
    private final String delimeter;

    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private CsvParser parser;
    private Duration frequency;
    private List<Integer> measures;

    private long size;
    private long meanByteSize;
    private boolean isInitialized = false;


    public CsvRandomAccessReader(String filePath,
                                 DateTimeFormatter formatter,
                                 Integer timeCol, String delimeter, Boolean hasHeader,
                                 List<Integer> measures) throws IOException {
        this(filePath, formatter, timeCol, delimeter, hasHeader, measures, Charset.defaultCharset());
    }

    public CsvRandomAccessReader(String filePath,
                                 DateTimeFormatter formatter,
                                 Integer timeCol, String delimeter, Boolean hasHeader,
                                 List<Integer> measures,
                                 Charset charset) throws IOException {
        super(new File(filePath), charset);
        this.filePath = filePath;
        this.formatter = formatter;
        this.timeCol = timeCol;
        this.delimeter = delimeter;
        this.hasHeader = hasHeader;
        this.measures = measures;
        this.csvParserSettings = this.createCsvParserSettings();
        this.parser = new CsvParser(csvParserSettings);
        this.isInitialized = true;
    }

    public Duration sample() throws IOException {
        if(hasHeader) this.readNewLine();
        this.startTime = parseStringToDate(parser.parseLine(this.readNewLine())[timeCol]);
        long startOffset = this.current;
        LocalDateTime secondTime = parseStringToDate(parser.parseLine(this.readNewLine())[0]);
        goToEnd();
        String lastLine = this.readNewLine();
        this.frequency = Duration.between(this.startTime, secondTime);
        this.endTime = parseStringToDate(parser.parseLine(lastLine)[0]);
        this.size = Duration.between(this.startTime, this.endTime).dividedBy(this.frequency);

        this.setDirection(DIRECTION.FORWARD);
        this.seek(startOffset);
        int sample_count = (int) (this.size * 0.2);
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

        this.meanByteSize = diff_sum / sample_count;
        return this.frequency;
    }

    private long findPosition(LocalDateTime time) { return Duration.between(this.startTime, time).dividedBy(this.frequency);}

    private long findProbabilisticOffset(long position) throws IOException {
        return Math.min(meanByteSize * position, (this.channel.size() - 100 * meanByteSize));
    }

    private long findOffset(LocalDateTime time) throws IOException {
        long position = findPosition(time);
        long probabilisticOffset = findProbabilisticOffset(position);

        this.setDirection(DIRECTION.FORWARD);
        this.seek(probabilisticOffset);

        String line = this.readNewLine();
        LocalDateTime firstTimeFound = parseStringToDate(this.parseLine(line)[0]);
        LocalDateTime timeFound;
        if (firstTimeFound.isAfter(time)) {
            this.setDirection(DIRECTION.BACKWARD);
            while(true) {
                long prevOffset = this.getFilePointer();
                line = this.readNewLine();
                timeFound = parseStringToDate(this.parseLine(line)[0]);
                if(timeFound.isBefore(time) || timeFound.isEqual(time)) return prevOffset;
            }
        }
        else {
            this.setDirection(DIRECTION.FORWARD);
            while(true) {
                long prevOffset = this.getFilePointer();
                line = this.readNewLine();
                timeFound = parseStringToDate(this.parseLine(line)[0]);
                if(timeFound.isAfter(time) || timeFound.isEqual(time)) return prevOffset - 1;
            }
        }
    }

    public void goToEnd() throws IOException {
        this.setDirection(DIRECTION.BACKWARD);
        this.seek(this.length() - 1);
    }

    public String[] parseNext() throws IOException {
        return this.parser.parseLine(this.readNewLine());
    }

    public String[] parseLine(String line) {
        return this.parser.parseLine(line);
    }

    public String[] parseHeader() throws IOException {
        this.parseNext();
        return this.parser.getContext().parsedHeaders();
    }

    private void updateMeasures(List<Integer> measures) {
        this.measures = measures;
        List<Integer> tmpList = new ArrayList<>();
        tmpList.add(this.timeCol);
        tmpList.addAll(this.measures);
        this.csvParserSettings.selectIndexes(tmpList.toArray(new Integer[0]));
        this.parser = new CsvParser(this.csvParserSettings);
    }

    public LocalDateTime parseStringToDate(String s) {return LocalDateTime.parse(s, formatter);}

    public ArrayList<String[]> getData(TimeRange range, List<Integer> measures) throws IOException {
        if(!measures.equals(this.measures)) updateMeasures(measures);
        ArrayList<String[]> rows = new ArrayList<>();
        long toOffset = findOffset(range.getTo());
        long fromOffset = findOffset(range.getFrom());
        this.seek(fromOffset);
        this.setDirection(DIRECTION.FORWARD);
        String line;
        while(this.getFilePointer() <= toOffset + 1){
            line = this.readNewLine();
            if(!line.isEmpty()) rows.add(parseLine(line));
        }
        return rows;
    }

    public String[] getData(LocalDateTime time, List<Integer> measures) throws IOException {
        if(!measures.equals(this.measures)) updateMeasures(measures);
        long position = findPosition(time);
        long probabilisticOffset = findProbabilisticOffset(position);

        this.setDirection(DIRECTION.FORWARD);
        this.seek(probabilisticOffset);

        String line = this.readNewLine();
        LocalDateTime firstTimeFound = parseStringToDate(parseLine(line)[0]);
        LocalDateTime timeFound;
        if (firstTimeFound.isAfter(time)) {
            this.setDirection(DIRECTION.BACKWARD);
            do {
                line = this.readNewLine();
                timeFound = parseStringToDate(this.parseLine(line)[0]);
            } while (!timeFound.isBefore(time) && !timeFound.isEqual(time));
        }
        else{
            this.setDirection(DIRECTION.FORWARD);
            do {
                line = this.readNewLine();
                timeFound = parseStringToDate(this.parseLine(line)[0]);
            } while (!timeFound.isAfter(time) && !timeFound.isEqual(time));
        }
        return parseLine(line);
    }

    private CsvParserSettings createCsvParserSettings() {
        CsvParserSettings parserSettings = new CsvParserSettings();
        parserSettings.getFormat().setDelimiter(this.delimeter.charAt(0));
        parserSettings.setIgnoreLeadingWhitespaces(false);
        parserSettings.setIgnoreTrailingWhitespaces(false);
        parserSettings.setLineSeparatorDetectionEnabled(true);
        parserSettings.setHeaderExtractionEnabled(false);
        List<Integer> tmpList = new ArrayList<>();
        tmpList.add(this.timeCol);
        tmpList.addAll(this.measures);
        parserSettings.selectIndexes(tmpList.toArray(new Integer[0]));
        return parserSettings;
    }

    public CsvParserSettings getCsvParserSettings() {
        return this.csvParserSettings;
    }
}
