package eu.more2020.visual.service;

import eu.more2020.visual.domain.*;
import eu.more2020.visual.domain.Dataset.CsvDataset;
import eu.more2020.visual.index.modelar.ModelarTTI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Service
public class IndexedModelarDataService {

    private final Logger log = LoggerFactory.getLogger(IndexedModelarDataService.class);

    private final ModelarDataService modelarDataService;


    private Map<String, ModelarTTI> indexes = new HashMap();

    public IndexedModelarDataService(ModelarDataService modelarDataService) {
        this.modelarDataService = modelarDataService;
    }

    public void removeIndex(String id) {
        indexes.remove(id);
    }

    private synchronized ModelarTTI getIndex(CsvDataset dataset) {
        ModelarTTI index = indexes.get(dataset.getId());
        if (index != null) {
            return index;
        }
        index = new ModelarTTI(modelarDataService, dataset);
        this.indexes.put(dataset.getId(), index);
        return index;
    }

    public QueryResults executeQuery(CsvDataset dataset, Query query) throws Exception {
        log.debug(query.toString());
        try {
            ModelarTTI index = this.getIndex(dataset);
            return index.executeQuery(query);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

}
