package eu.more2020.visual.util;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import eu.more2020.visual.domain.Query.AbstractQuery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

public class InfluxDBQueryExecutor implements QueryExecutor{

    private static final Logger LOG = LoggerFactory.getLogger(SQLQueryExecutor.class);


    InfluxDBClient influxDBClient;
    String table;
    String bucket;

    public InfluxDBQueryExecutor(InfluxDBClient influxDBClient, String bucket, String table) {
        this.influxDBClient = influxDBClient;
        this.table = table;
        this.bucket = bucket;
    }

    public void executeM4Query(AbstractQuery q) throws SQLException {
        String flux = String.format("from(bucket:\"%s\") " +
                "|> range(start:2018-02-03T00:18:00Z, stop:2018-09-05T00:18:00Z) " +
                "|> filter(fn: (r) => r[\"_field\"] == \"value\")" +
                "|> filter(fn: (r) => r[\"_measurement\"] == \"%s\") " +
                "|> aggregateWindow(every: 6h, fn: mean, createEmpty: false)" +
                "|> yield(name: \"mean\")", bucket, table);
        QueryApi queryApi = influxDBClient.getQueryApi();

        System.out.println(flux);
        List<FluxTable> tables = queryApi.query(flux);
        for (FluxTable fluxTable : tables) {

            List<FluxRecord> records = fluxTable.getRecords();

            for (FluxRecord fluxRecord : records) {

                System.out.println(fluxRecord.getValueByKey("sensor_id"));

            }

        }
    }
}
