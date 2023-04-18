package eu.more2020.visual.datasource;

import eu.more2020.visual.domain.Dataset.*;

public class DataSourceFactory {

    public static DataSource getDataSource(AbstractDataset dataset) {
        if (dataset instanceof CsvDataset) {
            return new CsvDataSource((CsvDataset) dataset);
        }
        else if(dataset instanceof ParquetDataset){
            return new ParquetDataSource((ParquetDataset) dataset);
        }else if(dataset instanceof PostgreSQLDataset)
            return new PostgreSQLDatasource((PostgreSQLDataset) dataset);
        else if(dataset instanceof InfluxDBDataset)
            return new InfluxDBDatasource((InfluxDBDataset) dataset);
        throw new IllegalArgumentException("Unsupported Datasource");
    }
}
