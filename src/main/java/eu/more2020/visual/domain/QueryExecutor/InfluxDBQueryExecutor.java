package eu.more2020.visual.domain.QueryExecutor;

import com.influxdb.client.*;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import eu.more2020.visual.domain.InfluxDB.InitQueries.BEBEZE;
import eu.more2020.visual.domain.Query.AbstractQuery;
import eu.more2020.visual.domain.Query.InfluxDBQuery;
import eu.more2020.visual.domain.Query.QueryMethod;
import eu.more2020.visual.domain.QueryResults;
import eu.more2020.visual.domain.UnivariateDataPoint;
import eu.more2020.visual.domain.InfluxDB.InitQueries.INTEL_LAB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

public class InfluxDBQueryExecutor implements QueryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBQueryExecutor.class);


    InfluxDBClient influxDBClient;
    String table;
    String bucket;
    String org;


    public InfluxDBQueryExecutor(InfluxDBClient influxDBClient, String org,
                                 String bucket, String table) {
        this.influxDBClient = influxDBClient;
        this.org = org;
        this.table = table;
        this.bucket = bucket;
    }

    @Override
    public QueryResults execute(AbstractQuery q, QueryMethod method) {
        switch (method) {
            case M4:
                return executeM4Query(q);
            case M4OLAP:
                return executeM4OLAPQuery(q);
            case RAW:
                return executeRawQuery(q);
            default:
                return executeM4Query(q);
        }
    }

    @Override
    public QueryResults executeM4Query(AbstractQuery q) {
        return executeM4InfluxQuery((InfluxDBQuery) q);
    }

    @Override
    public QueryResults executeM4OLAPQuery(AbstractQuery q) {
        return executeM4OLAPQuery((InfluxDBQuery) q);
    }

    @Override
    public QueryResults executeRawQuery(AbstractQuery q) {
        return executeRawInfluxQuery((InfluxDBQuery) q);
    }

    @Override
    public void initialize(String path) throws FileNotFoundException {
        WriteApi writeApi = influxDBClient.makeWriteApi();
        FileReader reader;
        if(table.equals("bebeze")) {
            reader = new FileReader(path);
            CsvToBean<BEBEZE> csvToBean = new CsvToBeanBuilder<BEBEZE>(reader)
                    .withType(BEBEZE.class)
                    .build();
            for (BEBEZE data : csvToBean) {
                writeApi.writeMeasurement(bucket, org, WritePrecision.S, data);
            }
        }
        else if(table.equals("intel_lab")){
            reader = new FileReader(path);
            CsvToBean<INTEL_LAB> csvToBean = new CsvToBeanBuilder<INTEL_LAB>(reader)
                    .withType(INTEL_LAB.class)
                    .build();
            for (INTEL_LAB data : csvToBean) {
                writeApi.writeMeasurement(bucket, org, WritePrecision.S, data);
            }
        }
        influxDBClient.close();
    }

    @Override
    public void drop() {
        OffsetDateTime start = OffsetDateTime.of(LocalDateTime.of(1970, 1, 1,
                0, 0, 0), ZoneOffset.UTC);
        OffsetDateTime stop = OffsetDateTime.now();
        String predicate = "_measurement=" + table;
        DeleteApi deleteApi = influxDBClient.getDeleteApi();
        deleteApi.delete(start, stop, predicate, bucket, org);
    }

    Comparator<UnivariateDataPoint> compareLists = new Comparator<UnivariateDataPoint>() {
        @Override
        public int compare(UnivariateDataPoint s1, UnivariateDataPoint s2) {
            if (s1==null && s2==null) return 0;//swapping has no point here
            if (s1==null) return  1;
            if (s2==null) return -1;
            return (int) (s1.getTimestamp() - s2.getTimestamp());
        }
    };

    private QueryResults executeM4InfluxQuery(InfluxDBQuery q) {
        QueryResults queryResults = new QueryResults();
        HashMap<Integer, List<UnivariateDataPoint>> data = new HashMap<>();
        String flux = String.format(q.m4QuerySkeleton(),
                bucket, q.getFromDate(), q.getToDate(), table, // first
                bucket, q.getFromDate(), q.getToDate(), table, // last
                bucket, q.getFromDate(), q.getToDate(), table, // min
                bucket, q.getFromDate(), q.getToDate(), table, // max
                bucket, q.getFromDate(), q.getToDate(), table); // mean
        QueryApi queryApi = influxDBClient.getQueryApi();
        LOG.info("Executing Query: \n" + flux);
        List<FluxTable> tables = queryApi.query(flux);
        for (FluxTable fluxTable : tables) {
            List<FluxRecord> records = fluxTable.getRecords();
            for (FluxRecord fluxRecord : records) {
                Integer fieldId = q.getMeasureNames().indexOf(fluxRecord.getField());
                data.computeIfAbsent(fieldId, k -> new ArrayList<>()).add(
                        new UnivariateDataPoint(Objects.requireNonNull(fluxRecord.getTime()).toEpochMilli(),
                                Double.parseDouble(Objects.requireNonNull(fluxRecord.getValue()).toString())));
            }
        }
        data.forEach((k, v) -> v.sort(compareLists));
        queryResults.setData(data);
        return queryResults;
    }

    private QueryResults executeM4OLAPQuery(InfluxDBQuery q) {
        return null;
    }

    private QueryResults executeRawInfluxQuery(InfluxDBQuery q) {
        QueryResults queryResults = new QueryResults();
        HashMap<Integer, List<UnivariateDataPoint>> data = new HashMap<>();
        String flux = String.format(q.rawQuerySkeleton(),
                bucket, q.getFromDate(), q.getToDate(), table);
        List<FluxTable> tables = execute(flux);
        for (FluxTable fluxTable : tables) {
            List<FluxRecord> records = fluxTable.getRecords();
            for (FluxRecord fluxRecord : records) {
                Integer fieldId = q.getMeasureNames().indexOf(fluxRecord.getField());
                data.computeIfAbsent(fieldId, k -> new ArrayList<>()).add(
                        new UnivariateDataPoint(Objects.requireNonNull(fluxRecord.getTime()).toEpochMilli(),
                                Double.parseDouble(Objects.requireNonNull(fluxRecord.getValue()).toString())));
            }
        }
        data.forEach((k, v) -> v.sort(compareLists));
        queryResults.setData(data);
        return queryResults;
    }


    public List<FluxTable> execute(String query) {
        QueryApi queryApi = influxDBClient.getQueryApi();
        LOG.info("Executing Query: \n" + query);
        return queryApi.query(query);
    }

}
