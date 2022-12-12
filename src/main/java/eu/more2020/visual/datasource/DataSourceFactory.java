package eu.more2020.visual.datasource;

import eu.more2020.visual.domain.Dataset.AbstractDataset;
import eu.more2020.visual.domain.Dataset.CsvDataset;
import eu.more2020.visual.domain.Dataset.ParquetDataset;

public class DataSourceFactory {

    public static DataSource getDataSource(AbstractDataset dataset) {
        if (dataset instanceof CsvDataset) {
            return new CsvDataSource((CsvDataset) dataset);
        }
        else if(dataset instanceof ParquetDataset){
            return new ParquetDataSource((ParquetDataset) dataset);
        }else
            return null;
    }
}
