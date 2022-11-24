package eu.more2020.visual.util.io;

import eu.more2020.visual.domain.TimeRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class ParquetReader {


    private final String filePath;
    private final ParquetFileReader reader;
    private final MessageType schema;
    private final DateTimeFormatter formatter;
    private final MessageColumnIO columnIO;
    private final Integer timeCol;
    private List<Integer> measures;
    private long startTime;

    private Duration frequency;
    private long size;
    private long partitionSize;

    public ParquetReader(String filePath,  Integer timeCol, List<Integer> measures, DateTimeFormatter formatter) throws IOException {
        this.filePath = filePath;
        this.timeCol = timeCol;
        this.measures = measures;
        this.formatter = formatter;
        this.reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(this.filePath), new Configuration()));
        this.schema = reader.getFooter().getFileMetaData().getSchema();
        this.size = reader.getRecordCount();
        this.columnIO = new ColumnIOFactory().getColumnIO(this.schema);
    }

    private int findProbabilisticRowGroupID(long time) {
        long index = findPosition(time);
        return (int) Math.floorDiv(index, partitionSize);
    }

    private int getRowGroupID(long time) throws IOException {
        int probabilisticRowGroupID = findProbabilisticRowGroupID(time);
        PageReadStore rowGroup = this.reader.readRowGroup(probabilisticRowGroupID);
        RecordReader rowGroupReader = columnIO.getRecordReader(rowGroup, new GroupRecordConverter(schema));
        SimpleGroup row = (SimpleGroup) rowGroupReader.read();
        long probabilisticTime =  parseStringToTimestamp(row.getBinary(timeCol, 0).toStringUsingUTF8());

        if (time < probabilisticTime) {
            while((probabilisticTime >= time)) {
                probabilisticRowGroupID --;
                rowGroup = this.reader.readRowGroup(probabilisticRowGroupID);
                rowGroupReader = columnIO.getRecordReader(rowGroup, new GroupRecordConverter(schema));
                row = (SimpleGroup) rowGroupReader.read();
                probabilisticTime = parseStringToTimestamp(row.getBinary(timeCol, 0).toStringUsingUTF8());
            }
            System.out.println(parseTimestampToString(time));
        }
        else if(time > probabilisticTime) {
            while((probabilisticTime <= time)) {
                probabilisticRowGroupID ++;
                rowGroup = this.reader.readRowGroup(probabilisticRowGroupID);
                rowGroupReader = columnIO.getRecordReader(rowGroup, new GroupRecordConverter(schema));
                row = (SimpleGroup) rowGroupReader.read();
                probabilisticTime = parseStringToTimestamp(row.getBinary(timeCol, 0).toStringUsingUTF8());
            }
            System.out.println(parseTimestampToString(time));
        }
        return Math.max(probabilisticRowGroupID - 1, 0);
    }

    private List<PageReadStore> getRowGroups(long start, long end) throws IOException {
        List<PageReadStore> rowGroups = new ArrayList<>();
        int startRowGroupID = getRowGroupID(start);
        int endRowGroupID = getRowGroupID(end);
        long i = startRowGroupID;
        while(i <= endRowGroupID){
            rowGroups.add(this.reader.readRowGroup((int) i));
            i ++;
        }
        return rowGroups;
    }

    public Duration sample() throws IOException{
        PageReadStore firstRowGroup = reader.readRowGroup(0);
        this.partitionSize = firstRowGroup.getRowCount();
        RecordReader firstRowGroupReader = columnIO.getRecordReader(firstRowGroup, new GroupRecordConverter(schema));
        SimpleGroup row = (SimpleGroup) firstRowGroupReader.read();
        this.startTime =  parseStringToTimestamp(row.getBinary(timeCol, 0).toStringUsingUTF8()); // Get date col
        row = (SimpleGroup) firstRowGroupReader.read();
        long secondTime =  parseStringToTimestamp(row.getBinary(timeCol, 0).toStringUsingUTF8());
        this.frequency = Duration.of(startTime - secondTime, ChronoUnit.MILLIS);
        return this.frequency;
    }

    public String[] getRow(SimpleGroup rowGroup){
        String[] row = new String[this.measures.size() + 1];
        String time = rowGroup.getBinary(timeCol, 0).toStringUsingUTF8();
        row[0] = time;
        int j = 1;
        for (Integer column : this.measures){
            row[j++] = String.valueOf(rowGroup.getDouble(column, 0));
        }
        return row;
    }

    public ArrayList<String[]> getData(TimeRange range, List<Integer> measures) throws IOException {
        if(!measures.equals(this.measures)) this.measures = measures;
        long fromPosition = findPosition(range.getFrom());
        long toPosition = findPosition(range.getTo());
        long steps = toPosition - fromPosition - 1;
        ArrayList<String[]> rows = new ArrayList<>();
        List<PageReadStore> rowGroups = getRowGroups(range.getFrom(), range.getTo());
        PageReadStore firstRowGroup = rowGroups.get(0);
        RecordReader<Group> firstRowGroupReader = columnIO.getRecordReader(firstRowGroup, new GroupRecordConverter(this.schema));
        int i = 0;
        while(i++ < fromPosition % partitionSize - 1) firstRowGroupReader.read();

        while(i++ < firstRowGroup.getRowCount()){
            final SimpleGroup simpleGroup = (SimpleGroup) firstRowGroupReader.read();
            rows.add(getRow(simpleGroup));
        }
        for (PageReadStore rowGroup : rowGroups.subList(1, rowGroups.size())){
            RecordReader<Group> rowGroupReader = columnIO.getRecordReader(rowGroup, new GroupRecordConverter(this.schema));
            i = 0;
            while(i++ < rowGroup.getRowCount()){
                final SimpleGroup simpleGroup = (SimpleGroup) rowGroupReader.read();
                rows.add(getRow(simpleGroup));
                if(rows.size() > steps) break;
            }
            if(rows.size() > steps) break;
        }
        return rows;
    }

    public long parseStringToTimestamp(String s) {
        ZonedDateTime zonedDateTime = LocalDateTime.parse(s, formatter).atZone(ZoneId.systemDefault());
        return zonedDateTime.toInstant().toEpochMilli();
    }

    public String parseTimestampToString(long t) {
        return formatter.format(Instant.ofEpochMilli(t).atZone(ZoneId.systemDefault()).toLocalDateTime());
    }

    public long findPosition(long time) { return Duration.of(this.startTime - time, ChronoUnit.MILLIS).dividedBy(this.frequency); }

    public void close() throws IOException { reader.close(); }


}
