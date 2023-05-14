package eu.more2020.visual.domain.Dataset;

import java.sql.SQLException;

public class ExperimentsDataset extends AbstractDataset{
    InfluxDBDataset influxDBDataset;
    PostgreSQLDataset postgreSQLDataset;


    public ExperimentsDataset(String postgreSQLCfg, String influxDBCfg, String schema, String table, String timeFormat) throws SQLException {
        influxDBDataset = new InfluxDBDataset(influxDBCfg, schema, table, timeFormat);
        postgreSQLDataset = new PostgreSQLDataset(postgreSQLCfg, schema, table, timeFormat);
    }


    public InfluxDBDataset getInfluxDBDataset() {
        return influxDBDataset;
    }

    public PostgreSQLDataset getPostgreSQLDataset() {
        return postgreSQLDataset;
    }
}
