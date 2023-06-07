package eu.more2020.visual.datasource.QueryExecutor;

import eu.more2020.visual.domain.Dataset.*;
import eu.more2020.visual.domain.Detection.PostgreSQL.PostgreSQLConnection;
import eu.more2020.visual.domain.InfluxDB.InfluxDBConnection;

public class QueryExecutorFactory {

    public static QueryExecutor getQueryExecutor(AbstractDataset dataset) {
        if (dataset instanceof CsvDataset) {
            return null;
        }
        else if(dataset instanceof ParquetDataset){
            return null;
        }else if(dataset instanceof PostgreSQLDataset) {
            PostgreSQLConnection postgreSQLConnection = new PostgreSQLConnection(((PostgreSQLDataset) dataset).getConfig());
            return postgreSQLConnection.getSqlQueryExecutor(dataset.getSchema(), dataset.getName());
        }
        else if(dataset instanceof InfluxDBDataset) {
            InfluxDBConnection influxDBConnection = new InfluxDBConnection(((InfluxDBDataset) dataset).getConfig());
            return influxDBConnection.getSqlQueryExecutor(dataset.getSchema(), dataset.getName(), dataset.getHeader());
        }
        throw new IllegalArgumentException("Unsupported Datasource");
    }
}
