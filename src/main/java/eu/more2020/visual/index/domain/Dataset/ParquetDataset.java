package eu.more2020.visual.index.domain.Dataset;

import eu.more2020.visual.index.domain.DataFileInfo;
import eu.more2020.visual.index.domain.TimeRange;
import eu.more2020.visual.index.util.io.ParquetReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class ParquetDataset extends AbstractDataset {

    private static final Logger LOG = LoggerFactory.getLogger(ParquetReader.class);
    private Integer timeColIndex;


    public ParquetDataset(String path, String id, String name,
                          String timeCol, String timeFormat) throws IOException {
        super(path, id, name, timeCol, timeFormat);
        this.fillParquetDatasetInfo();
        LOG.info("Initialized dataset: {}", this);
    }


    public void fillParquetDataFileInfo(DataFileInfo dataFileInfo) throws IOException {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(this.getTimeFormat());
        ParquetReader parquetReader = new ParquetReader(dataFileInfo.getFilePath(), formatter, getTimeCol());
        setSamplingInterval(parquetReader.getSamplingInterval());
        setTimeColIndex(parquetReader.getTimeColIndex());
        setHeader(parquetReader.getParsedHeader());
        dataFileInfo.setTimeRange(parquetReader.getTimeRange());
    }

    @Override
    public List<Integer> getMeasures(){
        int[] measures = new int[getHeader().length - 1];
        int i = 0;
        int j = i;
        while(i < getHeader().length - 1){
            if(i != getTimeColIndex()){
                measures[i] = j;
                i ++;
            }
            j++;
        }
        return Arrays.stream(measures)
                .boxed()
                .collect(Collectors.toList());
    }

    public void fillParquetDatasetInfo() throws IOException {
        File file = new File(this.getPath());
        if (!file.isDirectory()) {
            DataFileInfo dataFileInfo = new DataFileInfo(file.getAbsolutePath());
            fillParquetDataFileInfo(dataFileInfo);
            getFileInfoList().add(dataFileInfo);
        } else {
            List<DataFileInfo> fileInfoList = Arrays.stream(Objects.requireNonNull(file.listFiles(f -> !f.isDirectory() && f.getName()
                    .endsWith(".parquet")))).map(f -> {
                DataFileInfo dataFileInfo = new DataFileInfo(f.getAbsolutePath());
                try {
                    fillParquetDataFileInfo(dataFileInfo);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return dataFileInfo;
            }).sorted(Comparator.comparing(i -> i.getTimeRange().getFrom())).collect(Collectors.toList());
            // sort parquet files by their time ranges ascending
            this.setFileInfoList(fileInfoList);
        }
        if (fileInfoList != null && !fileInfoList.isEmpty()) {
            this.setTimeRange(new TimeRange(fileInfoList.get(0).getTimeRange()
                    .getFrom(), fileInfoList.get(fileInfoList.size() - 1).getTimeRange().getTo()));
        }
    }

    public Integer getTimeColIndex() {
        return timeColIndex;
    }

    public void setTimeColIndex(Integer timeColIndex) {
        this.timeColIndex = timeColIndex;
    }
}
