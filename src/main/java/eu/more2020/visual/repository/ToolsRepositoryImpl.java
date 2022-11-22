package eu.more2020.visual.repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.more2020.visual.config.ApplicationProperties;
import eu.more2020.visual.domain.*;

import eu.more2020.visual.domain.Detection.ChangepointDetection;
import eu.more2020.visual.domain.Detection.DeviationDetection;
import eu.more2020.visual.domain.Detection.RangeDetection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.stream.Collectors;

@Repository
public class ToolsRepositoryImpl implements ToolsRepository {

    private final ApplicationProperties applicationProperties;

    private final DateTimeFormatter formatter;

    private final Logger log = LoggerFactory.getLogger(ToolsRepositoryImpl.class);

    public ToolsRepositoryImpl(ApplicationProperties applicationProperties) {
        this.applicationProperties = applicationProperties;
        this.formatter =
            new DateTimeFormatterBuilder().appendPattern(applicationProperties.getTimeFormat())
                .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
                .toFormatter();
    }

    public List<String> toDateArray(Iterator<JsonNode> iterator) {

        List<String> dates = new ArrayList<>();
        while (iterator.hasNext()) {
            JsonNode dateNode = iterator.next();
            dates.add(dateNode.asText());
        }
        return dates;
    }

    @Override
    public List<Changepoint> getManualChangepoints(String id) {
        try {
            URL dataURL = new URL(applicationProperties.getToolApi() + "washes/" + id);
            HttpURLConnection con = (HttpURLConnection) dataURL.openConnection();
            con.setRequestMethod("POST");
            ObjectMapper objectMapper = new ObjectMapper();
            BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer content = new StringBuffer();
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }
            in.close();
            con.disconnect();
            JsonNode responseObject = objectMapper.readTree(content.toString());
            JsonNode starts = responseObject.get("Starting_date");
            JsonNode ends = responseObject.get("Ending_date");
            Integer noOfIntervals = starts.size();
            List<Changepoint> gtChangepoints = new ArrayList<>();
            for (Integer i = 0; i < noOfIntervals; i++) {
                String ii = i.toString();
                gtChangepoints.add(new Changepoint(i, new TimeRange(LocalDateTime.parse(starts.get(ii).asText(), formatter),
                    LocalDateTime.parse(ends.get(ii).asText(), formatter)), 0.0));
            }
            return gtChangepoints;
        } catch (Exception e) {
            e.printStackTrace();
            return new ArrayList<>();
        }
    }

    public JsonNode getRains(String id, Map<String, Object> params) throws IOException {
        URL dataURL = new URL(applicationProperties.getToolApi() + "rains/" + id);
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(params);
        HttpURLConnection conn = (HttpURLConnection) dataURL.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Accept", "application/json");
        conn.setDoOutput(true);
        try (OutputStream os = conn.getOutputStream()) {
            byte[] input = json.getBytes("utf-8");
            os.write(input, 0, input.length);
        }
        BufferedReader in = new BufferedReader(
            new InputStreamReader(conn.getInputStream()));
        String inputLine;
        StringBuffer content = new StringBuffer();
        while ((inputLine = in.readLine()) != null) {
            content.append(inputLine);
        }
        JsonNode responseObject = objectMapper.readTree(content.toString());

        in.close();
        return responseObject;
    }

    @Override
    public List<DataPoint> forecasting(String id){
        String jsonName = applicationProperties.getWorkspacePath() + "/" + id + "_predict.json";
        File json = new File(jsonName);
        List<DataPoint> forecastData = new ArrayList<>();
        if(json.exists()){
            // read json
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                BufferedReader in = new BufferedReader(
                    new FileReader(json));
                String inputLine;
                StringBuffer content = new StringBuffer();
                while ((inputLine = in.readLine()) != null) {
                    content.append(inputLine);
                }
                JsonNode responseObject = objectMapper.readTree(content.toString());
                Iterator<Map.Entry<String, JsonNode>> iter = responseObject.fields();
                while(iter.hasNext()){
                    Map.Entry<String, JsonNode> datum = iter.next();
                    double[] vals = new double[1];
                    vals[0] = datum.getValue().asDouble();
                    DataPoint dataPoint = new DataPoint(LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(datum.getKey())), ZoneId.systemDefault()),
                        vals);
                    forecastData.add(dataPoint);
                }
            }
            catch (Exception e){
                e.printStackTrace();
            }

        }
        return forecastData;
    }

    @Override
    public List<Changepoint> changepointDetection(ChangepointDetection changepointDetection) {
        List<Changepoint> detectedChangepoints = new ArrayList<>();
        try {
            URL dataURL = new URL(applicationProperties.getToolApi() + "cp_detection/" + changepointDetection.getId());
            Map<String, Object> params = new LinkedHashMap<>();
            params.put("start_date", changepointDetection.getRange().getFrom().format(formatter));
            params.put("end_date", changepointDetection.getRange().getTo().format(formatter));
            if (changepointDetection.getChangepoints() != null) {
                params.put("c_starts", changepointDetection.getChangepoints().stream().map(changepoint -> changepoint.getRange().getFrom().format(formatter)).collect(Collectors.toList()));
                params.put("c_ends", changepointDetection.getChangepoints().stream().map(changepoint -> changepoint.getRange().getTo().format(formatter)).collect(Collectors.toList()));
            }
            ObjectMapper objectMapper = new ObjectMapper();
            String json = objectMapper.writeValueAsString(params);
            HttpURLConnection conn = (HttpURLConnection) dataURL.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Accept", "application/json");
            conn.setDoOutput(true);
            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = json.getBytes("utf-8");
                os.write(input, 0, input.length);
            }
            BufferedReader in = new BufferedReader(
                new InputStreamReader(conn.getInputStream()));
            String inputLine;
            StringBuffer content = new StringBuffer();
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }
            in.close();
            JsonNode responseObject = objectMapper.readTree(content.toString());
            JsonNode scores = responseObject.get("Score");
            JsonNode starts = responseObject.get("Starting_date");
            JsonNode ends = responseObject.get("Ending_date");
            Integer noOfIntervals = starts.size();
            for (Integer i = 0; i < noOfIntervals; i++) {
                String ii = i.toString();
                detectedChangepoints.add(new Changepoint(i, new TimeRange(LocalDateTime.parse(starts.get(ii).asText(), formatter),
                    LocalDateTime.parse(ends.get(ii).asText(), formatter)), 0.0));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return detectedChangepoints;
    }

    @Override
    public List<DataPoint> soilingDetection(DeviationDetection deviationDetection) {
        List<DataPoint> dataPoints = new ArrayList<>();
        try {
            URL dataURL = new URL(applicationProperties.getToolApi() + "power_index/" + deviationDetection.getId());
            Map<String, Object> params = new LinkedHashMap<>();
            params.put("start_date", deviationDetection.getRange().getFrom().format(formatter));
            params.put("end_date", deviationDetection.getRange().getTo().format(formatter));
            if (deviationDetection.getChangepoints() != null) {
                params.put("cp_starts", deviationDetection.getChangepoints().stream().map(changepoint -> changepoint.getRange().getFrom().format(formatter)).collect(Collectors.toList()));
                params.put("cp_ends", deviationDetection.getChangepoints().stream().map(changepoint -> changepoint.getRange().getTo().format(formatter)).collect(Collectors.toList()));
            }
            params.put("weeks_train", deviationDetection.getWeeks());
            log.debug("DV for: " + params);
            ObjectMapper objectMapper = new ObjectMapper();
            String json = objectMapper.writeValueAsString(params);
            HttpURLConnection conn = (HttpURLConnection) dataURL.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Accept", "application/json");
            conn.setDoOutput(true);
            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = json.getBytes("utf-8");
                os.write(input, 0, input.length);
            }
            BufferedReader in = new BufferedReader(
                new InputStreamReader(conn.getInputStream()));
            String inputLine;
            StringBuffer content = new StringBuffer();

            // Write CSV to local file
            String[] header = in.readLine().split(","); // read header

            while ((inputLine = in.readLine()) != null) {
                String[] splitted = inputLine.split(",");
                DataPoint dataPoint = new DataPoint(LocalDateTime.parse(splitted[0], formatter),
                    Arrays.stream(splitted).skip(1).mapToDouble(Double::parseDouble).toArray());
                dataPoints.add(dataPoint);
            }
            in.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return dataPoints;
    }

    @Override
    public List<DataPoint> yawMisalignmentDetection(RangeDetection rangeDetection) {
        List<DataPoint> dataPoints = new ArrayList<>();
        try {
            URL dataURL = new URL(applicationProperties.getToolApi() + "yaw_misalignment/" + rangeDetection.getId());
            Map<String, Object> params = new LinkedHashMap<>();
            params.put("start_date", rangeDetection.getRange().getFrom().format(formatter));
            params.put("end_date", rangeDetection.getRange().getTo().format(formatter));

            log.debug("YAW for: " + params);
            ObjectMapper objectMapper = new ObjectMapper();
            String json = objectMapper.writeValueAsString(params);
            HttpURLConnection conn = (HttpURLConnection) dataURL.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Accept", "application/json");
            conn.setDoOutput(true);
            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = json.getBytes("utf-8");
                os.write(input, 0, input.length);
            }
            BufferedReader in = new BufferedReader(
                new InputStreamReader(conn.getInputStream()));
            String inputLine;
            StringBuffer content = new StringBuffer();

            // Write CSV to local file
            String[] header = in.readLine().split(","); // read header

            while ((inputLine = in.readLine()) != null) {
                String[] splitted = inputLine.split(",");
                DataPoint dataPoint = new DataPoint(LocalDateTime.parse(splitted[0], formatter),
                    Arrays.stream(splitted).skip(1).mapToDouble(Double::parseDouble).toArray());
                dataPoints.add(dataPoint);
            }
            in.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return dataPoints;
    }


}
