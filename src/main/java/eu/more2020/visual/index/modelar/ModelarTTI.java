package eu.more2020.visual.index.modelar;

import eu.more2020.visual.domain.*;
import eu.more2020.visual.domain.Dataset.CsvDataset;
import eu.more2020.visual.index.TimeSeriesIndexUtil;
import eu.more2020.visual.index.TreeNode;
import eu.more2020.visual.service.ModelarDataService;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class ModelarTTI {

    private static final Logger LOG = LogManager.getLogger(ModelarTTI.class);

    protected TreeNode root;
    @Autowired
    private final ModelarDataService modelarDataService;
    private CsvDataset dataset;
    private boolean isInitialized = false;
    private DateTimeFormatter formatter;

    public ModelarTTI(ModelarDataService modelarDataService, CsvDataset dataset) {
        this.modelarDataService = modelarDataService;
        this.dataset = dataset;
        this.formatter = DateTimeFormatter.ofPattern(dataset.getTimeFormat(), Locale.ENGLISH);
    }


    public void add(Stack<Integer> labels, Map<Integer, DoubleSummaryStatistics> statisticsMap) {
        if (root == null) {
            root = new TreeNode(0, 0);
        }
        TreeNode node = root;
        for (Integer label : labels) {
            TreeNode child = node.getOrAddChild(label);
            node = child;
        }
        node.setStats(statisticsMap);

    }

    public QueryResults initialize(Query q0) throws Exception {
        List<Integer> measures = dataset.getMeasures();

        int queryFrequencyLevel = TimeSeriesIndexUtil.getTemporalLevelIndex(q0.getFrequency()) + 1;

        List<TimeRange> intervals = new ArrayList<>();
        if (q0.getRange() != null){
            intervals.add(q0.getRange());
        }
        String sql = modelarDataService.getSqlQuery(measures, q0.getFrequency(), intervals);

        FlightStream flightStream = modelarDataService.executeSqlQuery(sql);

        while (flightStream.next()) {
            VectorSchemaRoot vsr = flightStream.getRoot();
            int rowCount = vsr.getRowCount();
            LocalDateTime timeStamp = null;
            Map<Integer, DoubleSummaryStatistics> statisticsMap = null;

            for (int row = 0; row < rowCount; row++) {
                LocalDateTime currentTimeStamp = Instant.ofEpochMilli(((TimeStampVector) vsr.getVector("ts")).get(row)).atZone(ZoneOffset.UTC).toLocalDateTime();
                if (!currentTimeStamp.equals(timeStamp)) {
                    if (timeStamp != null) {
                        Stack<Integer> labels = new Stack<>();
                        for (int i = 0; i < queryFrequencyLevel; i++) {
                            labels.add(timeStamp.get(TimeSeriesIndexUtil.TEMPORAL_HIERARCHY.get(i)));
                        }
                        this.add(labels, statisticsMap);
                    }
                    statisticsMap = new HashMap<>();
                    timeStamp = currentTimeStamp;
                }
                int tid = ((IntVector) vsr.getVector("tid")).get(row);
                Double sum = ((Float8Vector) vsr.getVector("sum")).get(row);
                Float min = ((Float4Vector) vsr.getVector("min")).get(row);
                Float max = ((Float4Vector) vsr.getVector("max")).get(row);
                Long count = ((BigIntVector) vsr.getVector("count")).get(row);
                statisticsMap.put(tid, new DoubleSummaryStatistics(count, min, max, sum));
                // System.out.println(tid + " | " + timeStamp + " | " + value);
            }
            flightStream.close();
        }

        isInitialized = true;
        return this.executeQuery(q0);
    }


    public synchronized QueryResults executeQuery(Query query) throws Exception {
        if (!isInitialized) {
            return initialize(query);
        }
        ModelarQueryProcessor queryProcessor = new ModelarQueryProcessor(query, dataset, this, modelarDataService);
        return queryProcessor.prepareQueryResults(root);
    }

    public Map<Integer, DoubleSummaryStatistics> getStats(List<TreeNode> queryNodes) {
        Map<Integer, DoubleSummaryStatistics> stats = new HashMap<>();
        for (TreeNode node : queryNodes) {
            node.getStats().forEach(
                (key, value) ->
                    stats.merge(key, value, (v1, v2) -> {
                        v1.combine(v2);
                        return v1;
                    }));
        }
        return stats;
    }

    public void traverse(TreeNode node) {
        LOG.debug(node);
        Collection<TreeNode> children = node.getChildren();
        if (children != null && !children.isEmpty()) {
            for (TreeNode child : children) {
                traverse(child);
            }
        }
    }

    public LocalDateTime parseStringToDate(String s) {
        return LocalDateTime.parse(s, formatter);
    }
}
