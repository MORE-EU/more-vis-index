package eu.more2020.visual.middleware.datasource;

import eu.more2020.visual.middleware.datasource.QueryExecutor.InfluxDBQueryExecutor;
import eu.more2020.visual.middleware.datasource.QueryExecutor.ModelarDBQueryExecutor;
import eu.more2020.visual.middleware.datasource.QueryExecutor.QueryExecutor;
import eu.more2020.visual.middleware.domain.Dataset.*;

public class DataSourceFactory {

    public static DataSource getDataSource(QueryExecutor queryExecutor, AbstractDataset dataset) {
       if(dataset instanceof PostgreSQLDataset)
            return new PostgreSQLDatasource((PostgreSQLDataset) dataset);
        else if(dataset instanceof InfluxDBDataset)
            return new InfluxDBDatasource((InfluxDBQueryExecutor) queryExecutor, (InfluxDBDataset) dataset);
        else if(dataset instanceof ModelarDBDataset)
            return new ModelarDBDatasource((ModelarDBQueryExecutor) queryExecutor, (ModelarDBDataset) dataset);
        throw new IllegalArgumentException("Unsupported Datasource");
    }
}
