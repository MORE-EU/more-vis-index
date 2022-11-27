package eu.more2020.visual.datasource;

import eu.more2020.visual.domain.Dataset.AbstractDataset;
import eu.more2020.visual.domain.Dataset.CsvDataset;

public class DataSourceFactory {

    public static DataSource getDataSource(AbstractDataset dataset) {
        if (dataset instanceof CsvDataset) {
            return new CsvDataSource((CsvDataset) dataset);
        } else
            return null;
    }
}
