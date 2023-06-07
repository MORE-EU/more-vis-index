package eu.more2020.visual.domain.InfluxDB;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import eu.more2020.visual.datasource.QueryExecutor.InfluxDBQueryExecutor;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class InfluxDBConnection {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBConnection.class);
    private String config;
    private InfluxDBClient client;
    private String token;
    private String org;
    private String url;
    private Properties properties  = new Properties();;

    public InfluxDBConnection(String influxDBCfg) {
        this.config = influxDBCfg;
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
            org = properties.getProperty("org");
            url = properties.getProperty("url");
            InfluxDBClientOptions options = InfluxDBClientOptions
                    .builder()
                    .url(url)
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

    private InfluxDBQueryExecutor createQueryExecutor(String bucket, String measurement, String[] header) {
        return new InfluxDBQueryExecutor(client, org, bucket, measurement, header);
    }

    public InfluxDBQueryExecutor getSqlQueryExecutor(String bucket, String measurement) {
        return this.createQueryExecutor(bucket, measurement, new String[]{});
    }

    public InfluxDBQueryExecutor getSqlQueryExecutor(String bucket, String measurement, String[] header) {
        return this.createQueryExecutor(bucket, measurement, header);
    }



}