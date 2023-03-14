package eu.more2020.visual.experiments.util.InfluxDB;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import eu.more2020.visual.domain.Dataset.AbstractDataset;
import eu.more2020.visual.experiments.util.QueryExecutor.InfluxDBQueryExecutor;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class InfluxDB {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDB.class);
    private String config;
    private InfluxDBClient client;
    private String bucket;
    private String token;
    private String org;
    private String url;
    private List<String> measureNames;
    private Properties properties  = new Properties();;

    public InfluxDB(List<String> measureNames, String influxDBCfg) {
        this.config = influxDBCfg;
        this.measureNames = measureNames;
        this.connect();
    }

    private void connect() {
        OkHttpClient.Builder okHttpClient = new OkHttpClient.Builder()
                .readTimeout(10, TimeUnit.MINUTES)
                .writeTimeout(30, TimeUnit.MINUTES)
                .connectTimeout(1, TimeUnit.MINUTES);
        try {
            InputStream inputStream
                    = getClass().getClassLoader().getResourceAsStream(config);
            properties.load(inputStream);
            token = properties.getProperty("token");
            bucket = properties.getProperty("bucket");
            org = properties.getProperty("org");
            url = properties.getProperty("url");
            InfluxDBClientOptions options = InfluxDBClientOptions
                    .builder()
                    .url(url)
                    .bucket(bucket)
                    .org(org)
                    .authenticateToken(token.toCharArray())
                    .okHttpClient(okHttpClient)
                    .build();
            client = InfluxDBClientFactory.create(options);
            LOG.info("Initialized InfluxDB connection");
        } catch
        (Exception e) {
            LOG.error(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    public InfluxDBQueryExecutor createQueryExecutor(String path, String table) {
        return new InfluxDBQueryExecutor(client, bucket, path, table, org, measureNames);
    }

}