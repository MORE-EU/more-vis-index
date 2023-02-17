package eu.more2020.visual.experiments.util.QueryExecutor;

import com.influxdb.client.*;
import com.influxdb.client.domain.Query;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import eu.more2020.visual.domain.Query.AbstractQuery;
import eu.more2020.visual.domain.Query.InfluxQLQuery;
import eu.more2020.visual.domain.Query.QueryMethod;
import eu.more2020.visual.experiments.util.InfluxDB.InitQueries.BEBEZE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.SQLException;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

public class InfluxDBQueryExecutor implements QueryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(SQLQueryExecutor.class);


    InfluxDBClient influxDBClient;
    String table;
    String path;
    String bucket;
    String org;


    public InfluxDBQueryExecutor(InfluxDBClient influxDBClient, String bucket, String path, String table, String org) {
        this.influxDBClient = influxDBClient;
        this.path = path;
        this.table = table;
        this.bucket = bucket;
        this.org = org;
    }

    @Override
    public void execute(AbstractQuery q, QueryMethod method) throws SQLException {
        switch (method) {
            case M4:
                executeM4Query(q);
        }
    }

    @Override
    public void executeM4Query(AbstractQuery q) {
        executeM4InfluxQuery((InfluxQLQuery) q);
    }


    @Override
    public void initialize() throws FileNotFoundException {
        WriteApi writeApi = influxDBClient.makeWriteApi();
        FileReader reader;
        if(table.equals("bebeze")) {
            reader = new FileReader(path);
            CsvToBean<BEBEZE> csvToBean = new CsvToBeanBuilder(reader)
                    .withType(BEBEZE.class)
                    .build();
            for (BEBEZE data : csvToBean) {
                writeApi.writeMeasurement(WritePrecision.S, data);
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

    private void executeM4InfluxQuery(InfluxQLQuery q) {
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
                System.out.println(fluxRecord.getValues());
            }
        }
    }


}
