package TTITest;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.more2020.visual.datasource.CsvDataSource;
import eu.more2020.visual.datasource.DataSource;
import eu.more2020.visual.datasource.DataSourceFactory;
import eu.more2020.visual.domain.*;
import eu.more2020.visual.domain.Dataset.AbstractDataset;
import eu.more2020.visual.domain.Dataset.CsvDataset;
import eu.more2020.visual.domain.Dataset.ParquetDataset;
import eu.more2020.visual.domain.csv.CsvDataPoint;
import eu.more2020.visual.index.TTI;
import eu.more2020.visual.util.DateTimeUtil;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class CsvTTITest {

    public String farmName;
    public String id;

    public TTI tti;

    public String workspacePath = "/opt/more-workspace";

    public String timeFormat = "yyyy-MM-dd[ HH:mm:ss]";
    public String delimiter = ",";

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
                if (datasetId.equals(id)) {
                    String name = d.get("name").asText();
                    String type = d.get("type").asText();
                    String path = workspacePath + "/" + farmName + "/" + d.get("path").asText();
                    switch (type) {
                        case "CSV":
                            Integer csvTimeCol = d.get("timeCol").asInt();
                            List<Integer> csvMeasures = new ArrayList<>();
                            List<String> csvStringMeasures = new ArrayList<>();
                            boolean hasHeader = d.get("hasHeader").asBoolean();
                            for (JsonNode measure : d.get("measures")) {
                                if (measure.canConvertToInt())
                                    csvMeasures.add(measure.asInt());
                                else
                                    csvStringMeasures.add(measure.asText());
                            }
                            CsvDataset csvDataset = new CsvDataset(path, datasetId, name, csvTimeCol, csvMeasures, hasHeader, timeFormat, delimiter);
                            if (csvStringMeasures.size() > 0) {
                                for (String csvStringMeasure : csvStringMeasures) {
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
                            ParquetDataset parquetDataset = new ParquetDataset(path, datasetId, name, timeFormat);
                            parquetDataset.convertMeasures(parquetTimeCol, parquetMeasures);
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

    private String getRow(String[] row) {
        StringBuilder r = new StringBuilder();
        for (String rr : row) {
            r.append(rr).append(",");
        }
        return r.toString();
    }

/*    @Test
    public void test_wind() throws IOException {
        farmName = "BEBEZE";
        id = "bbz1";
        CsvDataset csvDataset = (CsvDataset) getDataset();
        String csv = csvDataset.getFileInfoList().get(csvDataset.getFileInfoList().size() - 1).getFilePath();
        ViewPort viewPort = new ViewPort(800, 400);
        this.csvTTI = new CsvTTI(csv, Objects.requireNonNull(csvDataset));
        double startInit = System.currentTimeMillis();
        csvTTI.initialize();
        System.out.println("Initialization Time: " + (System.currentTimeMillis() - startInit) / 1000);
        System.out.println();
        double startTest = System.currentTimeMillis();

//        LocalDateTime testTime = LocalDateTime.parse("2018-11-08 23:59:56", csvTTI.getFormatter());
//        csvTTI.testRandomAccess(testTime);
//        System.out.println("Random 1st Search Time: " + (System.currentTimeMillis() - startTest) / 1000);
//        System.out.println();
//
//        startTest = System.currentTimeMillis();
//        testTime = LocalDateTime.parse("2019-02-08 13:39:13", csvTTI.getFormatter());
//        csvTTI.testRandomAccess(testTime);
//        System.out.println("Random 2nd Search Time : " + (System.currentTimeMillis() - startTest) / 1000);
//        System.out.println();
//
//        startTest = System.currentTimeMillis();
//        testTime = LocalDateTime.parse("2018-03-30 00:30:01", csvTTI.getFormatter());
//        csvTTI.testRandomAccess(testTime);
//        System.out.println("Random 3rd Search Time : " + (System.currentTimeMillis() - startTest) / 1000);
//        System.out.println();

        long startTime = LocalDateTime.parse("2018-03-01 00:00:00", csvTTI.getFormatter())
                .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        long endTime = LocalDateTime.parse("2018-03-08 00:00:00", csvTTI.getFormatter()).atZone(ZoneId.systemDefault())
                .toInstant().toEpochMilli();
        ArrayList<String[]> rows;
        startTest = System.currentTimeMillis();
        TimeRange timeRange = new TimeRange(startTime, endTime);
        rows = csvTTI.testRandomAccessRange(timeRange, csvDataset.getMeasures());
        System.out.println("1 Week Range Search Time (No of rows) " + rows.size() + ": " + (System.currentTimeMillis() - startTest) / 1000);
        System.out.println(getRow(rows.get(0)));
        System.out.println(getRow(rows.get(rows.size() - 1)));
        System.out.println();
        calculateM4(timeRange, csvDataset, viewPort);


//        startTime = LocalDateTime.parse("2018-03-01 01:15:01", csvTTI.getFormatter()).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();;
//        endTime = LocalDateTime.parse("2018-04-01 00:30:59", csvTTI.getFormatter()).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();;
//        startTest = System.currentTimeMillis();
//        timeRange = new TimeRange(startTime, endTime);
//        rows = csvTTI.testRandomAccessRange(timeRange, csvDataset.getMeasures());
//        System.out.println("1 Month Range Search Time (No of rows) " + rows.size() + ": " + (System.currentTimeMillis() - startTest) / 1000);
//        System.out.println(getRow(rows.get(0)));
//        System.out.println(getRow(rows.get(rows.size() - 1)));
//        System.out.println();
//        calculateM4(timeRange, csvDataset, viewPort);

//        ReversedLinesFileReader reversedLinesFileReader = new ReversedLinesFileReader(new File(csv), 65536, Charset.defaultCharset());
//
//        startTest = System.currentTimeMillis();
//        reversedLinesFileReader.readLines(1296000);
//        System.out.println("Similar reader test : " + (System.currentTimeMillis() - startTest) / 1000);

//        startTime = LocalDateTime.parse("2018-03-01 01:15:01", csvTTI.getFormatter()).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();;
//        endTime = LocalDateTime.parse("2018-04-01 00:30:59", csvTTI.getFormatter()).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();;
//        startTest = System.currentTimeMillis();
//        List<Integer> newMeasures = Arrays.stream(new int[]{1, 2, 3, 4, 5, 6, 7}).boxed().collect(Collectors.toList());
//        rows = csvTTI.testRandomAccessRange(new TimeRange(startTime, endTime), newMeasures);
//        System.out.println("1 Month Range Search Time (No of rows) " + rows.size() + ": " + (System.currentTimeMillis() - startTest) / 1000);
//        System.out.println(getRow(rows.get(0)));
//        System.out.println(getRow(rows.get(rows.size() - 1)));
//        System.out.println();
//
//        startTime = LocalDateTime.parse("2018-03-01 00:00:00", csvTTI.getFormatter());
//        endTime = LocalDateTime.parse("2019-03-01 00:00:00", csvTTI.getFormatter());
//        startTest = System.currentTimeMillis();
//        rows = csvTTI.testRandomAccessRange(new TimeRange(startTime, endTime), csvDataset.getMeasures());
//        System.out.println("1 Year Range Search Time (No of rows) " + rows.size() + ": " + (System.currentTimeMillis() - startTest) / 1000);
//        System.out.println(getRow(rows.get(0)));
//        System.out.println(getRow(rows.get(rows.size() - 1)));
//        System.out.println();

    }*/

    @Test
    public void test_wind() throws IOException {
        farmName = "bbz";
        id = "bbz3";
        CsvDataset csvDataset = (CsvDataset) getDataset();
        ViewPort viewPort = new ViewPort(800, 400);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timeFormat);
        long startTime = LocalDateTime.parse("2018-01-03 00:05:50", formatter)
                .atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        long endTime = LocalDateTime.parse("2018-01-03 05:33:18", formatter).atZone(ZoneId.of("UTC"))
                .toInstant().toEpochMilli();
        TimeRange timeRange = new TimeRange(startTime, endTime);

        DataSource dataSource = DataSourceFactory.getDataSource(csvDataset);
        DataPoints dataPoints = dataSource.getDataPoints(timeRange, csvDataset.getMeasures());
        // DataPoints dataPoints = dataSource.getAllDataPoints(csvDataset.getMeasures());
        for (DataPoint dataPoint : dataPoints){
            System.out.println(dataPoint);
        }

        DateTimeUtil.M4(timeRange, csvDataset.getSamplingInterval(), viewPort);
/*
        TTI tti = new TTI(csvDataset);
        tti.initialize(null);
*/

    }
    @Test
    public void test_solar() throws IOException {
        farmName = "solar";
        id = "eugene";
        File metadataFile = new File(workspacePath + "/" + farmName, id + ".csv");
        String csv = metadataFile.getAbsolutePath();
        CsvDataset csvDataset = (CsvDataset) getDataset();

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timeFormat);
        long startTime = LocalDateTime.parse("2013-01-01 16:00:00", formatter)
                .atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        long endTime = LocalDateTime.parse("2013-02-08 00:00:00", formatter).atZone(ZoneId.of("UTC"))
                .toInstant().toEpochMilli();
        TimeRange timeRange = new TimeRange(startTime, endTime);

        DataSource dataSource = DataSourceFactory.getDataSource(csvDataset);
        DataPoints dataPoints = dataSource.getDataPoints(timeRange, csvDataset.getMeasures());
        // DataPoints dataPoints = dataSource.getAllDataPoints(csvDataset.getMeasures());
        for (DataPoint dataPoint : dataPoints){
            System.out.println(dataPoint);
        }
    }


}
