package eu.more2020.visual.domain.Dataset;

import java.util.List;

public class ModelarDataset extends AbstractDataset{
    public ModelarDataset(String path, String id, String name, Integer timeCol, List<Integer> measures, String timeFormat) {
        super(path, id, name, timeCol, measures, timeFormat);
    }
}
