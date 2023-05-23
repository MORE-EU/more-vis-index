package eu.more2020.visual.domain.QueryExecutor;

import com.influxdb.client.*;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import eu.more2020.visual.domain.InfluxDB.InitQueries.*;
import eu.more2020.visual.domain.Query.AbstractQuery;
import eu.more2020.visual.domain.Query.InfluxDBQuery;
import eu.more2020.visual.domain.Query.QueryMethod;
import eu.more2020.visual.domain.QueryResults;
import eu.more2020.visual.domain.UnivariateDataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;

import java.sql.SQLException;
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
        return collect(executeM4InfluxQuery((InfluxDBQuery) q), ((InfluxDBQuery) q).getMeasureNames());
    }


    @Override
    public QueryResults executeM4MultiQuery(AbstractQuery q) throws SQLException {
        return collect(executeM4MultiInfluxQuery((InfluxDBQuery) q), ((InfluxDBQuery) q).getMeasureNames());
    }

    @Override
    public QueryResults executeM4OLAPQuery(AbstractQuery q) {
        return collect(executeM4OLAPQuery((InfluxDBQuery) q), ((InfluxDBQuery) q).getMeasureNames());
    }

    @Override
    public QueryResults executeRawQuery(AbstractQuery q) {
        return collect(executeRawInfluxQuery((InfluxDBQuery) q), ((InfluxDBQuery) q).getMeasureNames());
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
        else if(table.equals("soccer")){
            reader = new FileReader(path);
            CsvToBean<SOCCER> csvToBean = new CsvToBeanBuilder<SOCCER>(reader)
                    .withType(SOCCER.class)
                    .build();
            for (SOCCER data : csvToBean) {
                writeApi.writeMeasurement(bucket, org, WritePrecision.S, data);
            }
        }
        else if(table.equals("manufacturing")){
            reader = new FileReader(path);
            CsvToBean<MANUFACTURING> csvToBean = new CsvToBeanBuilder<MANUFACTURING>(reader)
                    .withType(MANUFACTURING.class)
                    .build();
            for (MANUFACTURING data : csvToBean) {
                writeApi.writeMeasurement(bucket, org, WritePrecision.S, data);
            }
        }
        else if(table.equals("intel_lab_exp")){
            reader = new FileReader(path);
            CsvToBean<INTEL_LAB_EXP> csvToBean = new CsvToBeanBuilder<INTEL_LAB_EXP>(reader)
                    .withType(INTEL_LAB_EXP.class)
                    .build();
            for (INTEL_LAB_EXP data : csvToBean) {
                writeApi.writeMeasurement(bucket, org, WritePrecision.S, data);
            }
        }
        else if(table.equals("soccer_exp")){
            reader = new FileReader(path);
            CsvToBean<SOCCER_EXP> csvToBean = new CsvToBeanBuilder<SOCCER_EXP>(reader)
                    .withType(SOCCER_EXP.class)
                    .build();
            for (SOCCER_EXP data : csvToBean) {
                writeApi.writeMeasurement(bucket, org, WritePrecision.S, data);
            }
        }
        else if(table.equals("manufacturing_exp")){
            reader = new FileReader(path);
            CsvToBean<MANUFACTURING_EXP> csvToBean = new CsvToBeanBuilder<MANUFACTURING_EXP>(reader)
                    .withType(MANUFACTURING_EXP.class)
                    .build();
            for (MANUFACTURING_EXP data : csvToBean) {
                writeApi.writeMeasurement(bucket, org, WritePrecision.S, data);
            }
        }
        else if(table.equals("synthetic1m")){
            reader = new FileReader(path);
            CsvToBean<SYNTHETIC1M> csvToBean = new CsvToBeanBuilder<SYNTHETIC1M>(reader)
                    .withType(SYNTHETIC1M.class)
                    .build();
            for (SYNTHETIC1M data : csvToBean) {
                writeApi.writeMeasurement(bucket, org, WritePrecision.S, data);
            }
        }
        else if(table.equals("synthetic2m")){
            reader = new FileReader(path);
            CsvToBean<SYNTHETIC2M> csvToBean = new CsvToBeanBuilder<SYNTHETIC2M>(reader)
                    .withType(SYNTHETIC2M.class)
                    .build();
            for (SYNTHETIC2M data : csvToBean) {
                writeApi.writeMeasurement(bucket, org, WritePrecision.S, data);
            }
        }
        else if(table.equals("synthetic4m")){
            reader = new FileReader(path);
            CsvToBean<SYNTHETIC4M> csvToBean = new CsvToBeanBuilder<SYNTHETIC4M>(reader)
                    .withType(SYNTHETIC4M.class)
                    .build();
            for (SYNTHETIC4M data : csvToBean) {
                writeApi.writeMeasurement(bucket, org, WritePrecision.S, data);
            }
        }
        else if(table.equals("synthetic8m")){
            reader = new FileReader(path);
            CsvToBean<SYNTHETIC8M> csvToBean = new CsvToBeanBuilder<SYNTHETIC8M>(reader)
                    .withType(SYNTHETIC8M.class)
                    .build();
            for (SYNTHETIC8M data : csvToBean) {
                writeApi.writeMeasurement(bucket, org, WritePrecision.S, data);
            }
        }
        else if(table.equals("synthetic16m")){
            reader = new FileReader(path);
            CsvToBean<SYNTHETIC16M> csvToBean = new CsvToBeanBuilder<SYNTHETIC16M>(reader)
                    .withType(SYNTHETIC16M.class)
                    .build();
            for (SYNTHETIC16M data : csvToBean) {
                writeApi.writeMeasurement(bucket, org, WritePrecision.S, data);
            }
        }
        else if(table.equals("synthetic32m")){
            reader = new FileReader(path);
            CsvToBean<SYNTHETIC32M> csvToBean = new CsvToBeanBuilder<SYNTHETIC32M>(reader)
                    .withType(SYNTHETIC32M.class)
                    .build();
            for (SYNTHETIC32M data : csvToBean) {
                writeApi.writeMeasurement(bucket, org, WritePrecision.S, data);
            }
        }
        else if(table.equals("synthetic64m")){
            reader = new FileReader(path);
            CsvToBean<SYNTHETIC64M> csvToBean = new CsvToBeanBuilder<SYNTHETIC64M>(reader)
                    .withType(SYNTHETIC64M.class)
                    .build();
            for (SYNTHETIC64M data : csvToBean) {
                writeApi.writeMeasurement(bucket, org, WritePrecision.S, data);
            }
        }
        else if(table.equals("synthetic128m")){
            reader = new FileReader(path);
            CsvToBean<SYNTHETIC128M> csvToBean = new CsvToBeanBuilder<SYNTHETIC128M>(reader)
                    .withType(SYNTHETIC128M.class)
                    .build();
            for (SYNTHETIC128M data : csvToBean) {
                writeApi.writeMeasurement(bucket, org, WritePrecision.S, data);
            }
        }
        else if(table.equals("synthetic128m")){
            reader = new FileReader(path);
            CsvToBean<SYNTHETIC128M> csvToBean = new CsvToBeanBuilder<SYNTHETIC128M>(reader)
                    .withType(SYNTHETIC128M.class)
                    .build();
            for (SYNTHETIC128M data : csvToBean) {
                writeApi.writeMeasurement(bucket, org, WritePrecision.S, data);
            }
        }
        else if(table.equals("synthetic256m")){
            reader = new FileReader(path);
            CsvToBean<SYNTHETIC256M> csvToBean = new CsvToBeanBuilder<SYNTHETIC256M>(reader)
                    .withType(SYNTHETIC256M.class)
                    .build();
            for (SYNTHETIC256M data : csvToBean) {
                writeApi.writeMeasurement(bucket, org, WritePrecision.S, data);
            }
        }
        else if(table.equals("synthetic512m")){
            reader = new FileReader(path);
            CsvToBean<SYNTHETIC512M> csvToBean = new CsvToBeanBuilder<SYNTHETIC512M>(reader)
                    .withType(SYNTHETIC512M.class)
                    .build();
            for (SYNTHETIC512M data : csvToBean) {
                writeApi.writeMeasurement(bucket, org, WritePrecision.S, data);
            }
        }
        else if(table.equals("synthetic1b")){
            reader = new FileReader(path);
            CsvToBean<SYNTHETIC1B> csvToBean = new CsvToBeanBuilder<SYNTHETIC1B>(reader)
                    .withType(SYNTHETIC1B.class)
                    .build();
            for (SYNTHETIC1B data : csvToBean) {
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

    public List<FluxTable> executeM4InfluxQuery(InfluxDBQuery q) {
        String flux = String.format(q.m4QuerySkeleton(),
                bucket, q.getFromDate(), q.getToDate(), table, // first
                bucket, q.getFromDate(), q.getToDate(), table, // last
                bucket, q.getFromDate(), q.getToDate(), table, // min
                bucket, q.getFromDate(), q.getToDate(), table ); // max
        return execute(flux);
    }

    public List<FluxTable> executeM4MultiInfluxQuery(InfluxDBQuery q){
        List<String> args = new ArrayList<>();

        for(int i = 0; i < q.getRanges().size(); i ++){
            for(int j = 0; j < 4; j++) {
                args.add(bucket);
                args.add(table);
            }
        }
        String flux = String.format(q.m4MultiQuerySkeleton(), args.toArray());
        return execute(flux);
    }

    public List<FluxTable> executeM4OLAPQuery(InfluxDBQuery q) {
        return null;
    }

    public List<FluxTable> executeRawInfluxQuery(InfluxDBQuery q) {
                String flux = String.format(q.rawQuerySkeleton(),
                bucket, q.getFromDate(), q.getToDate(), table);
        return execute(flux);
    }

    private QueryResults collect(List<FluxTable> tables, List<String> measureNames){
        QueryResults queryResults = new QueryResults();
        HashMap<Integer, List<UnivariateDataPoint>> data = new HashMap<>();
        for (FluxTable fluxTable : tables) {
            List<FluxRecord> records = fluxTable.getRecords();
            for (FluxRecord fluxRecord : records) {
                Integer fieldId = measureNames.indexOf(fluxRecord.getField());
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
