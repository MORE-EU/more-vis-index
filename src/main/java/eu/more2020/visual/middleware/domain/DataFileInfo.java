package eu.more2020.visual.middleware.domain;

public class DataFileInfo {

    String filePath;

    TimeRange timeRange;

    public DataFileInfo(String filePath) {
        this.filePath = filePath;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public TimeRange getTimeRange() {
        return timeRange;
    }

    public void setTimeRange(TimeRange timeRange) {
        this.timeRange = timeRange;
    }

    @Override
    public String toString() {
        return "DataFileInfo{" +
                "filePath='" + filePath + '\'' +
                ", timeRange=" + timeRange +
                '}';
    }
}
