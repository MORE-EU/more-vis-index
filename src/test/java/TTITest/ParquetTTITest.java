package TTITest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.more2020.visual.datasource.DataSource;
import eu.more2020.visual.datasource.DataSourceFactory;
import eu.more2020.visual.domain.DataPoint;
import eu.more2020.visual.domain.DataPoints;
import eu.more2020.visual.domain.Dataset.AbstractDataset;
import eu.more2020.visual.domain.Dataset.CsvDataset;
import eu.more2020.visual.domain.Dataset.ParquetDataset;
import eu.more2020.visual.domain.Farm;
import eu.more2020.visual.domain.TimeRange;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ParquetTTITest {

    public String farmName;
    public String id;


    public String workspacePath = "/opt/more-workspace";

    public String timeFormat = "yyyy-MM-dd[ HH:mm:ss]";
    public String delimiter = ",";

    final static Logger logger = LoggerFactory.getLogger(ParquetTTITest.class);

    private AbstractDataset getDataset() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Farm farm = new Farm();
        File metadataFile = new File(workspacePath + "/" + farmName, farmName + ".meta.json");
        if (metadataFile.exists()) {
            FileReader reader = new FileReader(metadataFile);
            JsonNode jsonNode = mapper.readTree(reader);
            farm.setName(jsonNode.get("name").toString());
            farm.setType(jsonNode.get("type").asInt());
            JsonNode data = jsonNode.get("data");
            for (JsonNode d : data) {
                String datasetId = d.get("id").asText();
                if(datasetId.equals(id)) {
                    String name = d.get("name").asText();
                    String type = d.get("type").asText();
                    String path = workspacePath + "/" + farmName +  "/" + d.get("path").asText();
                    switch (type){
                        case "CSV":
                            Integer csvTimeCol = d.get("timeCol").asInt();
                            List<Integer> csvMeasures = new ArrayList<>();
                            List<String> csvStringMeasures = new ArrayList<>();
                            boolean hasHeader = d.get("hasHeader").asBoolean();
                            for (JsonNode measure : d.get("measures")) {
                                if(measure.canConvertToInt())
                                    csvMeasures.add(measure.asInt());
                                else
                                    csvStringMeasures.add(measure.asText());
                            }
                            CsvDataset csvDataset = new CsvDataset(path, datasetId, name, csvTimeCol, csvMeasures, hasHeader, timeFormat, delimiter);
                            if(csvStringMeasures.size() > 0){
                                for (String csvStringMeasure : csvStringMeasures){
                                    csvMeasures.add(Arrays.asList(csvDataset.getHeader()).indexOf(csvStringMeasure));
                                }
                                csvDataset.setMeasures(csvMeasures);
                            }
                            return csvDataset;
                        case "parquet":
                            String parquetTimeCol = d.get("timeCol").asText();
                            List<String> parquetMeasures = new ArrayList<>();
                            for (JsonNode measure : d.get("measures")) {
                                parquetMeasures.add(measure.asText());
                            }
                            ParquetDataset parquetDataset = new ParquetDataset(path, datasetId, name, parquetTimeCol, parquetMeasures, timeFormat);
                            return parquetDataset;
                        case "modelar":
                            break;
                        default:
                            break;


                    }
                }
            }
        }
        return null;
    }

    private String getRow(String[] row){
        StringBuilder r = new StringBuilder();
        for (String rr : row){
            r.append(rr).append(",");
        }
        return r.toString();
    }

    @Test
    public void test_wind() throws IOException {
        farmName = "BEBEZE2";
        id = "bbz2";
        File metadataFile = new File(workspacePath + "/" + farmName, id + ".parquet");
        String filePath = metadataFile.getAbsolutePath();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timeFormat);

        double startInit = System.currentTimeMillis();
        ParquetDataset parquetDataset = (ParquetDataset) getDataset();
        System.out.println("Initialization Time: " + (System.currentTimeMillis() - startInit) / 1000);

        long startTime = LocalDateTime.parse("2018-04-01 00:04:41", formatter).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        long endTime = LocalDateTime.parse("2018-04-08 00:00:59", formatter).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        ArrayList<String[]> rows;
        double startTest = System.currentTimeMillis();
        TimeRange timeRange = new TimeRange(startTime, endTime);

        DataSource dataSource = DataSourceFactory.getDataSource(parquetDataset);
        DataPoints dataPoints = dataSource.getDataPoints(startTime, endTime, parquetDataset.getMeasures());
//         DataPoints dataPoints = dataSource.getAllDataPoints(csvDataset.getMeasures());
        System.out.println(timeRange);
        List<DataPoint> dataPoints1 = new ArrayList<>();
        for (DataPoint dataPoint : dataPoints){
            dataPoints1.add(dataPoint);
        }
        System.out.println(timeRange);
        System.out.println(dataPoints1.size());
        System.out.println(dataPoints1.get(0));
        System.out.println(dataPoints1.get(dataPoints1.size() - 1));

        System.out.println("1 Week Range Search Time : " + (System.currentTimeMillis() - startTest) / 1000);
//
        startTime = LocalDateTime.parse("2018-03-01 00:00:00", formatter).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        endTime = LocalDateTime.parse("2018-04-01 00:00:00", formatter).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        timeRange = new TimeRange(startTime, endTime);
        startTest = System.currentTimeMillis();
//
         dataSource = DataSourceFactory.getDataSource(parquetDataset);
         dataPoints = dataSource.getDataPoints(timeRange.getFrom(), timeRange.getTo(), parquetDataset.getMeasures());
//         DataPoints dataPoints = dataSource.getAllDataPoints(csvDataset.getMeasures());
        dataPoints1 = new ArrayList<>();
        for (DataPoint dataPoint : dataPoints){
            dataPoints1.add(dataPoint);
        }
        System.out.println(timeRange);
        System.out.println(dataPoints1.size());
        System.out.println(dataPoints1.get(0));
        System.out.println(dataPoints1.get(dataPoints1.size() - 1));

        System.out.println("1 Month Range Search Time : " + (System.currentTimeMillis() - startTest) / 1000);

    }

//
    @Test
    public void test_solar() throws IOException {
        farmName = "solar";
        id = "basil";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timeFormat);

        File metadataFile = new File(workspacePath + "/" + farmName, id + ".parquet");
        String filePath = metadataFile.getAbsolutePath();
        ParquetDataset parquetDataset = (ParquetDataset) getDataset();
        long startTime = LocalDateTime.parse("2013-02-01 00:30:00", formatter).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        long endTime = LocalDateTime.parse("2013-03-08 00:30:00", formatter).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        double startTest = System.currentTimeMillis();
        TimeRange timeRange = new TimeRange(startTime, endTime);

        DataSource dataSource = DataSourceFactory.getDataSource(parquetDataset);
        DataPoints dataPoints = dataSource.getDataPoints(timeRange.getFrom(), timeRange.getTo(), parquetDataset.getMeasures());
        List<DataPoint> dataPoints1 = new ArrayList<>();

        for (DataPoint dataPoint : dataPoints){
            dataPoints1.add(dataPoint);
        }

        System.out.println(timeRange);
        System.out.println(dataPoints1.size());
        System.out.println(dataPoints1.get(0));
        System.out.println(dataPoints1.get(dataPoints1.size() - 1));

    }
}
