package eu.more2020.visual.domain.Dataset;

import eu.more2020.visual.domain.DataFileInfo;
import eu.more2020.visual.domain.TimeRange;
import eu.more2020.visual.domain.ViewPort;

import java.time.Duration;

import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.time.temporal.ChronoUnit;
import java.util.*;

public abstract class AbstractDataset implements Serializable {


    private static final long serialVersionUID = 1L;
    private Duration samplingInterval;
    @NotNull
    private String path;
    @NotNull
    private String id;
    private String name;
    private Integer resType; // 0: panel, 1: turbine
    private String[] header;
    private Integer timeCol;
    private String timeFormat;
    private String farmName;
    private TimeRange timeRange;
    private String type;
    List<DataFileInfo> fileInfoList = new ArrayList<>();
    private List<Integer> measures;

    public AbstractDataset(String path, String id, String name, String timeFormat) {
        this.path = path;
        this.id = id;
        this.name = name;
        this.timeFormat = timeFormat;
    }

    public AbstractDataset(String path, String id, String name,Integer timeCol, List<Integer> measures,  String timeFormat) {
        this.path = path;
        this.id = id;
        this.name = name;
        this.timeCol = timeCol;
        this.measures = measures;
        this.timeFormat = timeFormat;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Integer getResType() {
        return resType;
    }

    public void setResType(Integer resType) {
        this.resType = resType;
    }

    public String getTimeFormat() {
        return timeFormat;
    }

    public void setTimeFormat(String timeFormat) {
        this.timeFormat = timeFormat;
    }

    public String getFarmName() {
        return farmName;
    }

    public void setFarmName(String farmName) {
        this.farmName = farmName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public TimeRange getTimeRange() {
        return timeRange;
    }

    public void setTimeRange(TimeRange timeRange) {
        this.timeRange = timeRange;
    }

    public String[] getHeader() { return header; }

    public void setHeader(String[] header) { this.header = header; }

    public String getType() { return type; }

    public void setType(String type) { this.type = type; }

    public List<Integer> getMeasures() {
        return measures;
    }

    public Integer getTimeCol() {
        return timeCol;
    }

    public void setTimeCol(Integer timeCol) {
        this.timeCol = timeCol;
    }

    public void setMeasures(List<Integer> measures){
        this.measures = measures;
    }


    public Duration getSamplingInterval() {
        return samplingInterval;
    }

    public void setSamplingInterval(Duration samplingInterval) {
        this.samplingInterval = samplingInterval;
    }

    public List<DataFileInfo> getFileInfoList() {
        return fileInfoList;
    }

    public void setFileInfoList(List<DataFileInfo> fileInfoList) {
        this.fileInfoList = fileInfoList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AbstractDataset)) {
            return false;
        }
        return id != null && id.equals(((AbstractDataset) o).id);
    }

    @Override
    public int hashCode() {
        return 31;
    }

    @Override
    public String toString() {
        return "AbstractDataset{" +
                "samplingInterval=" + samplingInterval +
                ", path='" + path + '\'' +
                ", id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", resType=" + resType +
                ", header=" + Arrays.toString(header) +
                ", timeCol=" + timeCol +
                ", timeFormat='" + timeFormat + '\'' +
                ", farmName='" + farmName + '\'' +
                ", timeRange=" + timeRange +
                ", type='" + type + '\'' +
                ", fileInfoList=" + fileInfoList +
                ", measures=" + measures +
                '}';
    }
}
