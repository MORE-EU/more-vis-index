package eu.more2020.visual.domain;

import com.opencsv.CSVWriter;
import org.apache.hadoop.util.hash.Hash;

import java.io.*;
import java.time.LocalDateTime;
import java.util.*;

public class QueryResults implements Serializable {

    private static final long serialVersionUID = 1L;

    private Map<Integer, List<UnivariateDataPoint>> data;

    private Map<Integer, DoubleSummaryStatistics> measureStats;

    private List<LocalDateTime> timeRange = new ArrayList<>();

    private int ioCount = 0;

    public List<LocalDateTime> getTimeRange() {
        return this.timeRange;
    }

    public void setTimeRange(List<LocalDateTime> timeRange) {
        this.timeRange = timeRange;
    }

    public Map<Integer, List<UnivariateDataPoint>> getData() {
        return data;
    }

    public void setData(Map<Integer, List<UnivariateDataPoint>> data) {
        this.data = data;
    }

    public Map<Integer, DoubleSummaryStatistics> getMeasureStats() {
        return measureStats;
    }

    public void setMeasureStats(Map<Integer, DoubleSummaryStatistics> measureStats) {
        this.measureStats = measureStats;
    }

    public int getIoCount() {
        return ioCount;
    }

    public void setIoCount(int ioCount) {
        this.ioCount = ioCount;
    }

    public void toCsv(String path){
        File file = new File(path);
        try {
            // create FileWriter object with file as parameter
            FileWriter outputFile = new FileWriter(file);

            // create CSVWriter object filewriter object as parameter
            CSVWriter writer = new CSVWriter(outputFile);

            // adding header to csv
            int index = 1;
            int noOfRows = 0;
            int noOfCols = data.size();
            String[] header = new String[getData().size() + 1];
            header[0] = "timestamp";
            for (Map.Entry<Integer, List<UnivariateDataPoint>> mapEntry : getData().entrySet()) {
                header[index] = String.valueOf(mapEntry.getKey());
                index++;
                noOfRows = mapEntry.getValue().size();
            }
            writer.writeNext(header);
            // add data to csv
            String[][] rows = new String[noOfRows][noOfCols + 1];
            int col = 1;
            for (Map.Entry<Integer, List<UnivariateDataPoint>> mapEntry : getData().entrySet()) {
                List<UnivariateDataPoint> dataPoints = mapEntry.getValue();
                int row = 0;
                for (UnivariateDataPoint dataPoint : dataPoints) {
                    rows[row][0] = String.valueOf(dataPoint.getTimestamp());
                    rows[row][col] = String.valueOf(dataPoint.getValue());
                    row ++;
                }
                col ++;
            }
            for (int row = 0; row < noOfRows; row ++)
                writer.writeNext(rows[row], false);
            // closing writer connection
            writer.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "QueryResults{" +
            "data=" + data +
            ", measureStats=" + measureStats +
            ", timeRange=" + timeRange +
            ", ioCount=" + ioCount +
            '}';
    }
}
