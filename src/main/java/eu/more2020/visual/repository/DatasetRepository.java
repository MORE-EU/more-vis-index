package eu.more2020.visual.repository;

import eu.more2020.visual.domain.Dataset.AbstractDataset;
import eu.more2020.visual.domain.Farm;
import eu.more2020.visual.domain.Sample;
import org.springframework.cache.annotation.Cacheable;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Repository for the Dataset entity.
 */
@SuppressWarnings("unused")
public interface DatasetRepository {

    String DATASETS_CACHE = "datasets";


    List<AbstractDataset> findAll() throws IOException;

    List<Sample> findSample(String farmName) throws IOException;

    List<String> findDirectories() throws IOException;

    Optional<Farm> findFarm(String farmName) throws IOException;

    @Cacheable(cacheNames = DATASETS_CACHE)
    Optional<AbstractDataset> findById(String id, String farmName) throws IOException;

    AbstractDataset save(AbstractDataset dataset) throws IOException;

    void deleteById(String id);


}
