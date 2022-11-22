package eu.more2020.visual.util.io;

import eu.more2020.visual.domain.Dataset.ParquetDataset;
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
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class ParquetReader {


    private final String filePath;
    private final ParquetDataset dataset;
    private final ParquetFileReader reader;
    private final MessageType schema;
    private final DateTimeFormatter formatter;
    private final MessageColumnIO columnIO;
    private LocalDateTime startTime;
    private Duration frequency;
    private List<Integer> measures;
    private long size;
    private long partitionSize;
    private boolean isInitialized = false;

    public ParquetReader(String filePath, ParquetDataset dataset, DateTimeFormatter formatter) throws IOException {
        this(filePath, dataset, dataset.getMeasures(), formatter);
    }
    public ParquetReader(String filePath, ParquetDataset dataset, List<Integer> measures, DateTimeFormatter formatter) throws IOException {
        this.filePath = filePath;
        this.dataset = dataset;
        this.measures = measures;
        this.formatter = formatter;
        this.reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(this.filePath), new Configuration()));
        this.schema = reader.getFooter().getFileMetaData().getSchema();
        this.size = reader.getRecordCount();
        this.columnIO = new ColumnIOFactory().getColumnIO(this.schema);
    }

    private int findProbabilisticRowGroupID(LocalDateTime time) {
        long index = findPosition(time);
        return (int) Math.floorDiv(index, partitionSize);
    }

    private int getRowGroupID(LocalDateTime time) throws IOException {
        int probabilisticRowGroupID = findProbabilisticRowGroupID(time);
        PageReadStore rowGroup = this.reader.readRowGroup(probabilisticRowGroupID);
        RecordReader rowGroupReader = columnIO.getRecordReader(rowGroup, new GroupRecordConverter(schema));
        SimpleGroup row = (SimpleGroup) rowGroupReader.read();
        LocalDateTime probabilisticTime =  parseStringToDate(row.getBinary(dataset.getTimeCol(), 0).toStringUsingUTF8());

        if (time.isBefore(probabilisticTime)) {
            while((probabilisticTime.isAfter(time) && !probabilisticTime.isEqual(time))) {
                probabilisticRowGroupID --;
                rowGroup = this.reader.readRowGroup(probabilisticRowGroupID);
                rowGroupReader = columnIO.getRecordReader(rowGroup, new GroupRecordConverter(schema));
                row = (SimpleGroup) rowGroupReader.read();
                probabilisticTime = parseStringToDate(row.getBinary(dataset.getTimeCol(), 0).toStringUsingUTF8());
            }
        }
        else if(time.isAfter(probabilisticTime)) {
            while((probabilisticTime.isBefore(time) && !probabilisticTime.isEqual(time))) {
                probabilisticRowGroupID ++;
                rowGroup = this.reader.readRowGroup(probabilisticRowGroupID);
                rowGroupReader = columnIO.getRecordReader(rowGroup, new GroupRecordConverter(schema));
                row = (SimpleGroup) rowGroupReader.read();
                probabilisticTime = parseStringToDate(row.getBinary(dataset.getTimeCol(), 0).toStringUsingUTF8());
            }
        }
        return Math.max(probabilisticRowGroupID - 1, 0);
    }

    private List<PageReadStore> getRowGroups(LocalDateTime start, LocalDateTime end) throws IOException {
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

    private void sample() throws IOException{
        PageReadStore firstRowGroup = reader.readRowGroup(0);
        this.partitionSize = firstRowGroup.getRowCount();
        RecordReader firstRowGroupReader = columnIO.getRecordReader(firstRowGroup, new GroupRecordConverter(schema));
        SimpleGroup row = (SimpleGroup) firstRowGroupReader.read();
        this.startTime =  parseStringToDate(row.getBinary(dataset.getTimeCol(), 0).toStringUsingUTF8()); // Get date col
        row = (SimpleGroup) firstRowGroupReader.read();
        LocalDateTime secondTime =  parseStringToDate(row.getBinary(dataset.getTimeCol(), 0).toStringUsingUTF8());
        this.frequency = Duration.between(startTime, secondTime);
    }

    public void initialize() throws IOException {
        sample();
        this.isInitialized = true;
    }

    public String[] getRow(SimpleGroup rowGroup){
        String[] row = new String[this.measures.size() + 1];
        String time = rowGroup.getBinary(dataset.getTimeCol(), 0).toStringUsingUTF8();
        row[0] = time;
        int j = 1;
        for (Integer column : this.measures){
            row[j++] = String.valueOf(rowGroup.getDouble(column, 0));
        }
        return row;
    }

    public ArrayList<String[]> getData(TimeRange range, List<Integer> measures) throws IOException {
        if(!isInitialized) this.initialize();
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

    public LocalDateTime parseStringToDate(String s) {return LocalDateTime.parse(s, formatter);}

    public long findPosition(LocalDateTime time) { return Duration.between(this.startTime, time).dividedBy(this.frequency); }

    public void close() throws IOException { reader.close(); }


}
