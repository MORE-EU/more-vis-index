package eu.more2020.visual.repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.bean.CsvToBeanBuilder;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import eu.more2020.visual.config.ApplicationProperties;
import eu.more2020.visual.domain.*;
import eu.more2020.visual.domain.Dataset.AbstractDataset;
import eu.more2020.visual.domain.Dataset.CsvDataset;
import eu.more2020.visual.service.ModelarDataService;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Repository
public class DatasetRepositoryImpl implements DatasetRepository {

    private final ApplicationProperties applicationProperties;

    private final Logger log = LoggerFactory.getLogger(DatasetRepositoryImpl.class);

    private final ModelarDataService modelarDataService;


    @Value("${application.timeFormat}")
    private String timeFormat;

    @Value("${application.delimiter}")
    private String delimiter;

    public DatasetRepositoryImpl(ApplicationProperties applicationProperties, ModelarDataService modelarDataService) {
        this.applicationProperties = applicationProperties;
        this.modelarDataService = modelarDataService;
    }

    @Override
    public List<AbstractDataset> findAll() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        List<AbstractDataset> datasets = new ArrayList<>();
        List<File> metadataFiles = Files.list(Paths.get(applicationProperties.getWorkspacePath()))
            .filter(path -> path.toString().endsWith(".meta.json")).map(Path::toFile).collect(Collectors.toList());
        for (File metadataFile : metadataFiles) {
            FileReader reader = new FileReader(metadataFile);
            datasets.add(mapper.readValue(reader, CsvDataset.class));
        }
        return datasets;
    }

    public Boolean hasWashes(String id) {
        try {
            URL dataURL = new URL(applicationProperties.getToolApi() + "washes/" + id);
            HttpURLConnection con = (HttpURLConnection) dataURL.openConnection();
            con.setRequestMethod("POST");
            int status = con.getResponseCode();
            BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer content = new StringBuffer();
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }
            in.close();
            con.disconnect();
            return Boolean.parseBoolean(String.valueOf(content));

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }


    @Override
    public Optional<Farm> findFarm(String farmName) throws IOException {
        Farm farm = null;
        ObjectMapper mapper = new ObjectMapper();
        File metadataFile = new File(applicationProperties.getWorkspacePath() + "/" + farmName, farmName + ".meta.json");

        if (metadataFile.exists()) {
            FileReader reader = new FileReader(metadataFile);
            farm = mapper.readValue(reader, Farm.class);
        }
        return Optional.ofNullable(farm);
    }

    @Override
    public Optional<AbstractDataset> findById(String id, String farmName) throws IOException {
        Assert.notNull(id, "Id must not be null!");
        ObjectMapper mapper = new ObjectMapper();
        CsvDataset dataset = null;
        List<AbstractDataset> allDatasets = null;
        Farm farm = new Farm();
        File metadataFile = new File(applicationProperties.getWorkspacePath() + "/" + farmName, farmName + ".meta.json");

        if (metadataFile.exists()) {
            FileReader reader = new FileReader(metadataFile);
            JsonNode jsonNode = mapper.readTree(reader);
            farm.setName(jsonNode.get("name").toString());
            farm.setType(jsonNode.get("type").asInt());
        }
        System.out.println(farm);
//        allDatasets = farm.getData();
//        for (CSVDataset d : allDatasets) {
//            if (d.getId().equals(id)) {
//                dataset = d;
//                dataset.setFarmName(farm.getName());
//                dataset.setResType(farm.getType());
//                if (dataset.getTimeFormat() == null || dataset.getTimeFormat().isEmpty()) {
//                    dataset.setTimeFormat(timeFormat);
//                }
//                if (dataset.getDelimiter() == null || dataset.getDelimiter().isEmpty()) {
//                    dataset.setDelimiter(delimiter);
//                }
//                break;
//            }
//        }
//        if (dataset != null) {
//            switch (dataset.getType()) {
//                case "modelar":
//                    try {
//                        modelarDataService.fillDatasetStats(dataset);
//                    } catch (Exception e) {
//                        throw new RuntimeException(e);
//                    }
//                    break;
//                default:
//                    File file = new File(applicationProperties.getWorkspacePath() + "/" + farmName, dataset.getName());
//                    if (!file.isDirectory()) {
//                        DataFileInfo dataFileInfo = new DataFileInfo(file.getAbsolutePath());
//                        fillDataFileInfo(dataset, dataFileInfo);
//                        dataset.getFileInfoList().add(dataFileInfo);
//                    } else {
//                        CSVDataset finalDataset = dataset;
//                        List<DataFileInfo> fileInfoList = Arrays.stream(file.listFiles(f -> !f.isDirectory() && f.getName().endsWith(".csv"))).map(f -> {
//                            DataFileInfo dataFileInfo = new DataFileInfo(f.getAbsolutePath());
//                            try {
//                                fillDataFileInfo(finalDataset, dataFileInfo);
//                            } catch (IOException e) {
//                                new RuntimeException(e);
//                            }
//                            return dataFileInfo;
//                        }).collect(Collectors.toList());
//                        // sort csv files by their time ranges ascending
//                        fileInfoList.sort(Comparator.comparing(i -> i.getTimeRange().getFrom()));
//                        dataset.setFileInfoList(fileInfoList);
//                    }
//                    if (dataset.getTimeRange() == null) {
//                        dataset.setTimeRange(dataset.getFileInfoList().get(0).getTimeRange());
//                    }
//                    dataset.setTimeRange(dataset.getFileInfoList().stream().map(DataFileInfo::getTimeRange)
//                        .reduce(dataset.getTimeRange(), (range1, range2) -> range1.span(range2)));
//                    break;
//            }
//        }
        return Optional.ofNullable(dataset);
    }

    private void fillDataFileInfo(CsvDataset dataset, DataFileInfo dataFileInfo) throws IOException {
        CsvParserSettings parserSettings = new CsvParserSettings();
        parserSettings.getFormat().setDelimiter(',');
        parserSettings.setIgnoreLeadingWhitespaces(false);
        parserSettings.setIgnoreTrailingWhitespaces(false);
        CsvParser parser = new CsvParser(parserSettings);
        parser.beginParsing(new File(dataFileInfo.getFilePath()), Charset.forName("US-ASCII"));
        if (dataset.getHasHeader()) {
            parser.parseNext();  //skip header row
            dataset.setHeader(parser.getContext().parsedHeaders());
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(dataset.getTimeFormat(), Locale.ENGLISH);
        LocalDateTime from = LocalDateTime.parse(parser.parseNext()[dataset.getTimeCol()], formatter);
        parser.stopParsing();

        ReversedLinesFileReader reversedLinesFileReader = new ReversedLinesFileReader(new File(dataFileInfo.getFilePath()), Charset.forName("US-ASCII"));
        String lastRow = reversedLinesFileReader.readLine();
        reversedLinesFileReader.close();
        LocalDateTime to = LocalDateTime.parse(parser.parseLine(lastRow)[dataset.getTimeCol()], formatter);
        dataFileInfo.setTimeRange(new TimeRange(from, to));
    }

    @Override
    public AbstractDataset save(AbstractDataset dataset) throws IOException {
        Assert.notNull(dataset, "Dataset must not be null!");
        ObjectMapper mapper = new ObjectMapper();
        File metadataFile = new File(applicationProperties.getWorkspacePath(), dataset.getId() + ".meta.json");
        FileWriter writer = new FileWriter(metadataFile);
        mapper.writeValue(writer, CsvDataset.class);
        return dataset;
    }

/*    @Override
    public List<String> findFiles(String folder) throws IOException {
        File file = new File(applicationProperties.getWorkspacePath() + "/" + folder);
        FileFilter fileFilter = f -> !f.isDirectory() && f.getName().endsWith(".csv") && !f.getName().contains("sample");
        File[] fileNames = file.listFiles(fileFilter);
        List<String> fileList = new ArrayList<>();
        for (File newFile : fileNames) {
            fileList.add(newFile.getName().toString());
        }
        return fileList;
    }*/

    @Override
    public List<Sample> findSample(String farmName) throws IOException {
        File f = new File(applicationProperties.getWorkspacePath() + "/" + farmName);
        File[] matchingFiles = f.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.contains("sample") && name.endsWith("csv");
            }
        });
        List<Sample> beans = new CsvToBeanBuilder(new FileReader(matchingFiles[0]))
            .withType(Sample.class).build().parse();
        return beans;
    }

    public List<String> findDirectories() throws IOException {
        File file = new File(applicationProperties.getWorkspacePath());
        String[] names = file.list();
        List<String> dirs = new ArrayList<>();

        for (String name : names) {

            if (new File(applicationProperties.getWorkspacePath() + "/" + name).isDirectory()) {
                log.debug(name);
                dirs.add(name);
            }
        }
        return dirs;
    }


    @Override
    public void deleteById(String id) {
        throw new UnsupportedOperationException();
    }
}
