package eu.more2020.visual.util;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

import eu.more2020.visual.domain.Query.AbstractQuery;
import eu.more2020.visual.domain.Query.InfluxQLQuery;
import eu.more2020.visual.domain.Query.QueryMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class InfluxDBQueryExecutor implements QueryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(SQLQueryExecutor.class);


    InfluxDBClient influxDBClient;
    String table;
    String bucket;

    public InfluxDBQueryExecutor(InfluxDBClient influxDBClient, String bucket, String table) {
        this.influxDBClient = influxDBClient;
        this.table = table;
        this.bucket = bucket;
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
//        for (FluxTable fluxTable : tables) {
//            List<FluxRecord> records = fluxTable.getRecords();
//            for (FluxRecord fluxRecord : records) {
//                System.out.println(fluxRecord.getValues());
//            }
//        }
    }


}
