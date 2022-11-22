package TTITest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.more2020.visual.config.ApplicationProperties;
import eu.more2020.visual.domain.Dataset.AbstractDataset;
import eu.more2020.visual.domain.Dataset.CsvDataset;
import eu.more2020.visual.domain.Dataset.ParquetDataset;
import eu.more2020.visual.domain.Farm;
import eu.more2020.visual.domain.TimeRange;
import eu.more2020.visual.index.parquet.ParquetTTI;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.Assert;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@ActiveProfiles("test")
@RunWith(SpringRunner.class)
@SpringBootTest(classes = ApplicationProperties.class)
@EnableConfigurationProperties
public class ParquetTTITest {

    @Autowired
    private ApplicationProperties applicationProperties;

    @Value("${application.timeFormat}")
    private String timeFormat;

    @Value("${application.delimiter}")

    private String delimiter;
    public String farmName;
    public String id;

    private AbstractDataset getDataset() throws IOException {
        Assert.notNull(id, "Id must not be null!");
        ObjectMapper mapper = new ObjectMapper();
        Farm farm = new Farm();
        File metadataFile = new File(applicationProperties.getWorkspacePath() + "/" + farmName, farmName + ".meta.json");
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
                    String path = applicationProperties.getWorkspacePath() + "/" + farmName +  "/" + d.get("path").asText();
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

        File metadataFile = new File(applicationProperties.getWorkspacePath() + "/" + farmName, id + ".parquet");
        String filePath = metadataFile.getAbsolutePath();
        double startInit = System.currentTimeMillis();
        ParquetDataset parquetDataset = (ParquetDataset) getDataset();
        ParquetTTI parquetTTI = new ParquetTTI(filePath, Objects.requireNonNull(parquetDataset));
        parquetTTI.initialize();
        System.out.println("Initialization Time: " + (System.currentTimeMillis() - startInit) / 1000);

        LocalDateTime startTime = LocalDateTime.parse("2018-04-01 00:04:41", parquetTTI.getFormatter());
        LocalDateTime endTime = LocalDateTime.parse("2018-04-08 00:00:59", parquetTTI.getFormatter());
        ArrayList<String[]> rows;
        double startTest = System.currentTimeMillis();
        rows = parquetTTI.testRandomAccessRange(new TimeRange(startTime, endTime), parquetDataset.getMeasures());
        System.out.println(getRow(rows.get(0)));
        System.out.println(getRow(rows.get(rows.size() - 1)));
        System.out.println("1 Week Range Search Time : " + (System.currentTimeMillis() - startTest) / 1000);

        startTime = LocalDateTime.parse("2018-03-01 00:00:00", parquetTTI.getFormatter());
        endTime = LocalDateTime.parse("2018-04-01 00:00:00", parquetTTI.getFormatter());
        startTest = System.currentTimeMillis();
        rows = parquetTTI.testRandomAccessRange(new TimeRange(startTime, endTime), parquetDataset.getMeasures());
        System.out.println(getRow(rows.get(0)));
        System.out.println(getRow(rows.get(rows.size() - 1)));
        System.out.println("1 Month Range Search Time : " + (System.currentTimeMillis() - startTest) / 1000);

    }


    @Test
    public void test_solar() throws IOException {
        farmName = "solar";
        id = "basil";

        File metadataFile = new File(applicationProperties.getWorkspacePath() + "/" + farmName, id + ".parquet");
        String filePath = metadataFile.getAbsolutePath();
        double startInit = System.currentTimeMillis();
        ParquetDataset parquetDataset = (ParquetDataset) getDataset();
        ParquetTTI parquetTTI = new ParquetTTI(filePath, Objects.requireNonNull(parquetDataset));
        parquetTTI.initialize();
        System.out.println("Initialization Time: " + (System.currentTimeMillis() - startInit) / 1000);

        LocalDateTime startTime = LocalDateTime.parse("2013-01-01 00:04:41", parquetTTI.getFormatter());
        LocalDateTime endTime = LocalDateTime.parse("2013-01-08 00:00:59", parquetTTI.getFormatter());
        ArrayList<String[]> rows;
        double startTest = System.currentTimeMillis();
        rows = parquetTTI.testRandomAccessRange(new TimeRange(startTime, endTime), parquetDataset.getMeasures());
        System.out.println(getRow(rows.get(0)));
        System.out.println(getRow(rows.get(rows.size() - 1)));
        System.out.println("1 Week Range Search Time : " + (System.currentTimeMillis() - startTest) / 1000);

        startTime = LocalDateTime.parse("2013-03-01 00:00:00", parquetTTI.getFormatter());
        endTime = LocalDateTime.parse("2013-04-01 00:00:00", parquetTTI.getFormatter());
        startTest = System.currentTimeMillis();
        rows = parquetTTI.testRandomAccessRange(new TimeRange(startTime, endTime), parquetDataset.getMeasures());
        System.out.println(getRow(rows.get(0)));
        System.out.println(getRow(rows.get(rows.size() - 1)));
        System.out.println("1 Month Range Search Time : " + (System.currentTimeMillis() - startTest) / 1000);

    }
}
