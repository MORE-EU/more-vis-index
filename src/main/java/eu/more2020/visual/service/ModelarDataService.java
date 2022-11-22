package eu.more2020.visual.service;

import eu.more2020.visual.domain.*;
import eu.more2020.visual.domain.Dataset.CsvDataset;
import eu.more2020.visual.domain.Dataset.ModelarDataset;
import eu.more2020.visual.index.TimeSeriesIndexUtil;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class ModelarDataService {

    private final Logger log = LoggerFactory.getLogger(ModelarDataService.class);

    @Value("${application.modelardb.url}")
    private String modelarUrl = "leviathan.imsi.athenarc.gr";

    @Value("${application.modelardb.port}")
    private Integer modelarPort = 9999;

    public String getSqlQuery(List<Integer> measures, String frequency, List<TimeRange> timeRanges) {
        String sql = "SELECT tid,  DATE_TRUNC('" + frequency + "', timestamp) AS ts, COUNT(1) AS count, MIN(value) min, MAX(value) max, SUM(value) sum FROM DataPoint "
            + " WHERE tid IN ("
            + measures.stream().map(i -> i.toString()).collect(Collectors.joining(",")) + ") ";
        if (timeRanges != null && timeRanges.size() > 0) {
            sql += " AND (";
            sql += timeRanges.stream().map(timeRange -> " (timestamp >= '"
                    + TimeSeriesIndexUtil.truncate(timeRange.getFrom(), frequency).toInstant(ZoneOffset.UTC) + "' AND timestamp < '"
                    + TimeSeriesIndexUtil.truncate(timeRange.getTo().plus(1, TimeSeriesIndexUtil.getTemporalFieldByName(frequency).getBaseUnit()), frequency).toInstant(ZoneOffset.UTC) + "') ")
                .collect(Collectors.joining(" OR "));
            sql += ") ";
        }
        sql += " GROUP BY tid, ts ORDER BY ts, tid";
        return sql;
    }

    public QueryResults executeQuery(ModelarDataset dataset, Query query) throws Exception {
        log.debug(query.toString());
        Location location = Location.forGrpcInsecure(modelarUrl, modelarPort);
        List<Integer> measures = query.getMeasures() != null ? query.getMeasures() : dataset.getMeasures();

        List<TimeRange> intervals = new ArrayList<>();
        if (query.getRange() != null) {
            intervals.add(query.getRange());
        }
        String sql = getSqlQuery(measures, query.getFrequency(), intervals);

        FlightStream flightStream = this.executeSqlQuery(sql);


        QueryResults queryResults = new QueryResults();
        List<DataPoint> dataPoints = new ArrayList<>();

        // System.out.println(flightStream.getSchema());
        while (flightStream.next()) {
            VectorSchemaRoot vsr = flightStream.getRoot();
            int rowCount = vsr.getRowCount();

            DataPoint dataPoint = null;
            LocalDateTime timeStamp = null;
            Map<Integer, Double> measureMap = null;
            for (int row = 0; row < rowCount; row++) {
                LocalDateTime currentTimeStamp = Instant.ofEpochMilli(((TimeStampVector) vsr.getVector("ts")).get(row)).atZone(ZoneOffset.UTC).toLocalDateTime();
                if (!currentTimeStamp.equals(timeStamp)) {
                    if (timeStamp != null) {
                        Map<Integer, Double> finalMeasureMap = measureMap;
                        dataPoint = new DataPoint(timeStamp, measures.stream().mapToDouble(m -> finalMeasureMap.get(m)).toArray());
                        dataPoints.add(dataPoint);
                    }
                    measureMap = new HashMap<>();
                    timeStamp = currentTimeStamp;
                }
                int tid = ((IntVector) vsr.getVector("tid")).get(row);
                Double sum = ((Float8Vector) vsr.getVector("sum")).get(row);
                Long count = ((BigIntVector) vsr.getVector("count")).get(row);
                measureMap.put(tid, sum / count);
                // System.out.println(tid + " | " + timeStamp + " | " + value);
            }
            flightStream.close();
        }

        queryResults.setData(dataPoints);
        queryResults.setMeasureStats(dataset.getMeasureStats());
        queryResults.setTimeRange(dataset.getTimeRange().toList());

        return queryResults;
    }

    public FlightStream executeSqlQuery(String sql) throws Exception {
        Location location = Location.forGrpcInsecure(modelarUrl, modelarPort);
        log.debug("Executing SQL query: " + sql);
        RootAllocator rootAllocator = new RootAllocator();
        FlightClient flightClient = FlightClient.builder()
            .location(location).allocator(rootAllocator).build();
        //Query
        Ticket ticket = new Ticket(sql.getBytes());
        return flightClient.getStream(ticket);
    }

    public void fillDatasetStats(CsvDataset dataset) throws Exception {
        List<Integer> measures = dataset.getMeasures();

        String sql = "SELECT tid, MIN(value) as min_value, MAX(value) as max_value, SUM(value) as sum_value, COUNT(1) as count, MIN(timestamp) as min_timestamp, MAX(timestamp) as max_timestamp FROM DataPoint "
            + " WHERE tid IN ("
            + measures.stream().map(i -> i.toString()).collect(Collectors.joining(",")) + ")  GROUP BY tid";

        FlightStream flightStream = this.executeSqlQuery(sql);

        Map<Integer, DoubleSummaryStatistics> measureStatsMap = new HashMap<>();

        TimeRange timeRange = null;

        while (flightStream.next()) {
            VectorSchemaRoot vsr = flightStream.getRoot();
            int rowCount = vsr.getRowCount();
            for (int row = 0; row < rowCount; row++) {
                if (timeRange == null) {
                    timeRange = new TimeRange();
                    timeRange.setFrom(Instant.ofEpochMilli(((TimeStampVector) vsr.getVector("min_timestamp")).get(row)).atZone(ZoneOffset.UTC).toLocalDateTime());
                    timeRange.setTo(Instant.ofEpochMilli(((TimeStampVector) vsr.getVector("max_timestamp")).get(row)).atZone(ZoneOffset.UTC).toLocalDateTime());
                }
                int tid = ((IntVector) vsr.getVector("tid")).get(row);
                DoubleSummaryStatistics measureStats = new DoubleSummaryStatistics(((BigIntVector) vsr.getVector("count")).get(row),
                    ((Float4Vector) vsr.getVector("min_value")).get(row),
                    ((Float4Vector) vsr.getVector("max_value")).get(row),
                    ((Float8Vector) vsr.getVector("sum_value")).get(row));
                measureStatsMap.put(tid, measureStats);
            }
            flightStream.close();
        }

        // dataset.setMeasureStats(measureStatsMap);
        dataset.setTimeRange(timeRange);
    }

}
