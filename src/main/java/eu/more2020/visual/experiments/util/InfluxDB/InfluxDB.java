package eu.more2020.visual.experiments.util.InfluxDB;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import eu.more2020.visual.experiments.util.QueryExecutor.InfluxDBQueryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public class InfluxDB {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDB.class);
    private String config;
    private InfluxDBClient client;
    private String bucket;
    private String token;
    private String org;
    private String url;
    private Properties properties;

    public InfluxDB(String influxDBCfg) {
        config = influxDBCfg;
        this.connect();
    }

    private void connect() {
        try {
            InputStream inputStream
                    = getClass().getClassLoader().getResourceAsStream(config);
            properties.load(inputStream);
            token = properties.getProperty("token");
            bucket = properties.getProperty("bucket");
            org = properties.getProperty("org");
            url = properties.getProperty("url");
            client = InfluxDBClientFactory.create(url, token.toCharArray(), org, bucket);
            LOG.info("Initialized InfluxDB connection");
        } catch
        (Exception e) {
            LOG.error(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    public InfluxDBQueryExecutor createQueryExecutor(String table) {
        return new InfluxDBQueryExecutor(client, bucket, table);
    }

}