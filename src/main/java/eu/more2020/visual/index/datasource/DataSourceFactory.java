package eu.more2020.visual.index.datasource;

import eu.more2020.visual.index.domain.Dataset.*;
import eu.more2020.visual.index.domain.Dataset.*;

public class DataSourceFactory {

    public static DataSource getDataSource(AbstractDataset dataset) {
       if(dataset instanceof PostgreSQLDataset)
            return new PostgreSQLDatasource((PostgreSQLDataset) dataset);
        else if(dataset instanceof InfluxDBDataset)
            return new InfluxDBDatasource((InfluxDBDataset) dataset);
        else if(dataset instanceof ModelarDBDataset)
            return new ModelarDBDatasource((ModelarDBDataset) dataset);
        throw new IllegalArgumentException("Unsupported Datasource");
    }
}
