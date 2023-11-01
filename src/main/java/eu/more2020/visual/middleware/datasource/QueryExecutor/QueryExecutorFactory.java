package eu.more2020.visual.middleware.datasource.QueryExecutor;

import eu.more2020.visual.middleware.domain.Dataset.*;
import eu.more2020.visual.middleware.domain.ModelarDB.ModelarDBConnection;
import eu.more2020.visual.middleware.domain.PostgreSQL.JDBCConnection;
import eu.more2020.visual.middleware.domain.InfluxDB.InfluxDBConnection;

public class QueryExecutorFactory {

    public static QueryExecutor getQueryExecutor(AbstractDataset dataset) {
        if(dataset instanceof PostgreSQLDataset) {
            JDBCConnection postgreSQLConnection = new JDBCConnection(((PostgreSQLDataset) dataset).getConfig());
            return postgreSQLConnection.getSqlQueryExecutor(dataset.getSchema(), dataset.getTable());
        }
        else if(dataset instanceof InfluxDBDataset) {
            InfluxDBConnection influxDBConnection = new InfluxDBConnection(((InfluxDBDataset) dataset).getConfig());
            return influxDBConnection.getSqlQueryExecutor(dataset.getSchema(), dataset.getTable(), dataset.getHeader());
        }
        else if(dataset instanceof ModelarDBDataset) {
            ModelarDBConnection modelarDBConnection = new ModelarDBConnection(((ModelarDBDataset) dataset).getConfig());
            return modelarDBConnection.getSqlQueryExecutor(dataset);
        }
        throw new IllegalArgumentException("Unsupported Datasource");
    }
}