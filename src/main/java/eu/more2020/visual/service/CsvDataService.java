package eu.more2020.visual.service;

import eu.more2020.visual.domain.Dataset.CsvDataset;
import eu.more2020.visual.domain.Query;
import eu.more2020.visual.domain.QueryResults;
import eu.more2020.visual.domain.TimeRange;
import eu.more2020.visual.index.csv.CsvTTI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class CsvDataService {

    private final Logger log = LoggerFactory.getLogger(CsvDataService.class);
    private HashMap<String, CsvTTI> indexes = new HashMap<>();


    public void removeIndex(String id, String farmName) {
        indexes.remove(id, farmName);
    }

    private synchronized List<CsvTTI> getIndexes(CsvDataset dataset, Query query) throws IOException {
        List<CsvTTI> ttis = dataset.getFileInfoList().stream().filter(dataFileInfo -> dataFileInfo.getTimeRange().intersects(query.getRange()))
            .map(dataFileInfo -> {
                CsvTTI tti = indexes.get(dataFileInfo.getFilePath());
                if (tti == null) {
                    tti = new CsvTTI(dataFileInfo.getFilePath(), dataset);
                    this.indexes.put(dataFileInfo.getFilePath(), tti);
                }
                return tti;
            }).collect(Collectors.toList());
        return ttis;
    }

    public TimeRange getTimeRangeInit (TimeRange timeRange, CsvDataset dataset) {
        if(Duration.between(timeRange.getFrom(), timeRange.getTo()).toDays() >= 1){
            return new TimeRange(dataset.getTimeRange().getTo().minus(8, ChronoUnit.DAYS), dataset.getTimeRange().getTo().minus(1, ChronoUnit.DAYS));
        }else{
            return new TimeRange(dataset.getTimeRange().getTo().minus(7, ChronoUnit.DAYS), dataset.getTimeRange().getTo());
        }
    }

    public QueryResults executeQuery(CsvDataset dataset, Query query) {
        log.debug(query.toString());
        if (query.getRange() == null) {
            query.setRange(getTimeRangeInit(dataset.getTimeRange(), dataset));
        }
        QueryResults queryResults = new QueryResults();
        queryResults.setTimeRange(dataset.getTimeRange().toList());
        queryResults.setMeasureStats(new HashMap<>());

        try {
            List<CsvTTI> ttis = this.getIndexes(dataset, query);
            for (CsvTTI tti : ttis) {
                QueryResults partialResults = tti.executeQuery(query);
                queryResults.getData().addAll(partialResults.getData());
                partialResults.getMeasureStats().forEach((measure, statistics) -> {
                    DoubleSummaryStatistics currentStatistics = queryResults.getMeasureStats().get(measure);
                    if (currentStatistics == null) {
                        queryResults.getMeasureStats().put(measure, statistics);
                    } else {
                        currentStatistics.combine(statistics);
                    }
                });
            }
            // queryResults.getMeasureStats().forEach((integer, doubleSummaryStatistics) -> log.debug(doubleSummaryStatistics.toString()));
            return queryResults;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
