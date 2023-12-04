package eu.more2020.visual.middleware.domain.InfluxDB;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import eu.more2020.visual.middleware.datasource.QueryExecutor.InfluxDBQueryExecutor;
import eu.more2020.visual.middleware.domain.Dataset.AbstractDataset;
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
        InputStream inputStream
                = getClass().getClassLoader().getResourceAsStream(config);
        try {
            properties.load(inputStream);
            token = properties.getProperty("token");
            org = properties.getProperty("org");
            url = properties.getProperty("url");
        }
        catch (Exception e) {
            LOG.error(e.getClass().getName() + ": " + e.getMessage());
        }
        this.connect();
    }

    public InfluxDBConnection(String url, String org, String token) {
        this.url = url;
        this.org = org;
        this.token = token;
        this.connect();
    }

    private void connect() {
        OkHttpClient.Builder okHttpClient = new OkHttpClient.Builder()
                .readTimeout(10, TimeUnit.MINUTES)
                .writeTimeout(30, TimeUnit.MINUTES)
                .connectTimeout(1, TimeUnit.MINUTES);
        InfluxDBClientOptions options = InfluxDBClientOptions
                .builder()
                .url(url)
                .org(org)
                .authenticateToken(token.toCharArray())
                .okHttpClient(okHttpClient)
                .build();
        client = InfluxDBClientFactory.create(options);
        LOG.info("Initialized InfluxDB connection");

    }

    private InfluxDBQueryExecutor createQueryExecutor(AbstractDataset dataset) {
        return new InfluxDBQueryExecutor(client, dataset);
    }

    private InfluxDBQueryExecutor createQueryExecutor() {
        return new InfluxDBQueryExecutor(client);
    }

    public InfluxDBQueryExecutor getSqlQueryExecutor() {
        return this.createQueryExecutor();
    }

    public InfluxDBQueryExecutor getSqlQueryExecutor(AbstractDataset dataset) {
        return this.createQueryExecutor(dataset);
    }



    public InfluxDBClient getClient() {
        return client;
    }

    public String getToken() {
        return token;
    }

    public String getOrg() {
        return org;
    }

    public String getUrl() {
        return url;
    }
}