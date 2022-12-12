package eu.more2020.visual.domain.Dataset;

import eu.more2020.visual.domain.DataFileInfo;
import eu.more2020.visual.domain.TimeRange;
import eu.more2020.visual.util.io.ParquetReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class ParquetDataset extends AbstractDataset{
    private static final Logger LOG = LoggerFactory.getLogger(ParquetReader.class);

    private List<String> measureNames;
    private String timeColName;

    public ParquetDataset(String path, String id, String name,
                          String timeColName, List<String> measureNames, String timeFormat) throws IOException {
       super(path, id, name, timeFormat);
       this.measureNames = measureNames;
       this.timeColName = timeColName;
       this.fillParquetDatasetInfo();
       LOG.info("Initialized dataset: {}", this);
    }

    public void fillParquetDataFileInfo(DataFileInfo dataFileInfo) throws IOException {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(this.getTimeFormat());
        ParquetReader parquetReader = new ParquetReader(dataFileInfo.getFilePath(), formatter, timeColName, measureNames);
        setSamplingInterval(parquetReader.getSamplingInterval());
        setHeader(parquetReader.getParsedHeader());
        setMeasures(parquetReader.getMeasures());
        setTimeCol(parquetReader.getTimeCol());
        dataFileInfo.setTimeRange(parquetReader.getTimeRange());
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
}
