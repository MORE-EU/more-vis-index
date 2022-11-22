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

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalUnit;
import java.util.*;
import java.util.stream.Collectors;

import static eu.more2020.visual.index.TimeSeriesIndexUtil.TEMPORAL_HIERARCHY;

public class ModelarQueryProcessor {

    private static final Logger LOG = LogManager.getLogger(ModelarQueryProcessor.class);
    private Query query;
    private QueryResults queryResults;
    private CsvDataset dataset;
    private int freqLevel;
    private Stack<TreeNode> stack = new Stack<>();
    private ModelarTTI timeseriesTreeIndex;
    private List<Integer> measures;
    private ModelarDataService modelarDataService;

    public ModelarQueryProcessor(Query query, CsvDataset dataset, ModelarTTI timeseriesTreeIndex, ModelarDataService modelarDataService) {
        this.query = query;
        this.measures = query.getMeasures() != null ? query.getMeasures() : dataset.getMeasures();
        this.queryResults = new QueryResults();
        this.queryResults.setMeasureStats(dataset.getMeasureStats());
        this.queryResults.setTimeRange(dataset.getTimeRange().toList());
        this.dataset = dataset;
        this.timeseriesTreeIndex = timeseriesTreeIndex;
        this.modelarDataService = modelarDataService;
        this.freqLevel = TimeSeriesIndexUtil.getTemporalLevelIndex(query.getFrequency()) + 1;
    }

    public QueryResults prepareQueryResults(TreeNode root) throws Exception {
        LocalDateTime start;
        LocalDateTime end;
        if (query.getRange() == null) {
            start = dataset.getTimeRange().getTo()
                .minus(7, ChronoUnit.DAYS);
            end = dataset.getTimeRange().getTo();
        } else {
            start = query.getRange().getFrom();
            end = query.getRange().getTo();
        }
        List<Integer> startLabels = getLabels(start);
        List<Integer> endLabels = getLabels(end);

        this.processQueryNodes(root, startLabels, endLabels, true, true, 0);


        start = TimeSeriesIndexUtil.truncate(start, query.getFrequency());
        end = TimeSeriesIndexUtil.truncate(end, query.getFrequency());

        List<TimeRange> missingIntervals = new ArrayList<>();
        List<DataPoint> dataPoints = queryResults.getData();
        LocalDateTime current_from = null;
        LocalDateTime current_to = null;
        for (LocalDateTime date = start; date.isBefore(end) || date.isEqual(end); date = date.plus(1, TimeSeriesIndexUtil.getTemporalFieldByName(query.getFrequency()).getBaseUnit())) {
            LocalDateTime finalDate = date;
            if (dataPoints.stream().anyMatch(dataPoint -> dataPoint.getTimestamp().equals(finalDate))) {
                if (current_from != null) {
                    missingIntervals.add(new TimeRange(current_from, current_to));
                    current_from = null;
                    current_to = null;
                }
            } else {
                if (current_from == null) {
                    current_from = date;
                }
                current_to = date;
            }
        }

        if (missingIntervals.isEmpty())
            return queryResults;

        int queryFrequencyLevel = TimeSeriesIndexUtil.getTemporalLevelIndex(query.getFrequency()) + 1;

        String sql = modelarDataService.getSqlQuery(dataset.getMeasures(), query.getFrequency(), missingIntervals);


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
                        Map<Integer, DoubleSummaryStatistics> finalStatisticsMap = statisticsMap;
                        DataPoint dataPoint = new DataPoint(timeStamp, measures.stream().mapToDouble(m -> finalStatisticsMap.get(m).getAverage()).toArray());
                        dataPoints.add(dataPoint);

                        Stack<Integer> labels = new Stack<>();
                        for (int i = 0; i < queryFrequencyLevel; i++) {
                            labels.add(timeStamp.get(TEMPORAL_HIERARCHY.get(i)));
                        }
                        timeseriesTreeIndex.add(labels, statisticsMap);
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
            }
            flightStream.close();
        }
        return queryResults;
    }

    public void processNode(TreeNode treeNode) throws IOException {
        TemporalUnit temporalUnit = TimeSeriesIndexUtil.getTemporalFieldByName(query.getFrequency()).getBaseUnit();
        LocalDateTime dateTime = getCurrentNodeDateTime(treeNode.getLevel()).truncatedTo(temporalUnit);

        if (treeNode.getLevel() == freqLevel) {
            if (treeNode.hasStats()) {
                queryResults.getData().add(new DataPoint(dateTime, measures.stream().mapToDouble(measure -> treeNode.getStats().get(measure).getAverage()).toArray()));
            } else if (treeNode.getChildren() != null) {
                try {
                    TemporalUnit nextTemporalUnit = TimeSeriesIndexUtil.TEMPORAL_HIERARCHY.get(TimeSeriesIndexUtil.getTemporalLevelIndex(query.getFrequency()) + 1).getBaseUnit();
                    if (treeNode.getChildren().size() == dateTime.until(dateTime.plus(1l, temporalUnit), nextTemporalUnit) &&
                        treeNode.getChildren().stream().allMatch(child -> child.hasStats())) {
                        treeNode.getChildren().forEach(child -> treeNode.adjustStats(child.getStatisticsMap()));
                        queryResults.getData().add(new DataPoint(dateTime, measures.stream().mapToDouble(measure -> treeNode.getStats().get(measure).getAverage()).toArray()));
                    }
                } catch (IndexOutOfBoundsException e) {
                }
            }
        }
    }

    private void processQueryNodes(TreeNode node, List<Integer> startLabels,
                                   List<Integer> endLabels, boolean isFirst, boolean isLast, int level) throws IOException {
        stack.push(node);
        Collection<TreeNode> children = node.getChildren();
        if (node.getLevel() == freqLevel || children == null || children.isEmpty()) {
            processNode(node);
            stack.pop();
            return;
        }

        // these are the children's filters
        int start = startLabels.get(level);
        int end = endLabels.get(level);
        /* We filter in each level only in the first node and the last. If we are on the first node, we get everything that is from the start filter
         * and after. Else if we are in the last node we get everything before the end filter. Finally, if we re in intermediary nodes we get all children
         * that are below the filtered values of the current node.*/
        if (isFirst)
            children = children.stream().filter(child -> child.getLabel() >= start).collect(Collectors.toList());
        if (isLast)
            children = children.stream().filter(child -> child.getLabel() <= end).collect(Collectors.toList());

        for (TreeNode child : children) {
            // The child's first node will be the first node of the current first node and the same for the end
            boolean childIsFirst = child.getLabel() == start && isFirst;
            boolean childIsLast = child.getLabel() == end && isLast;
            processQueryNodes(child, startLabels, endLabels, childIsFirst, childIsLast, level + 1);
        }
        stack.pop();
    }

    private LocalDateTime getCurrentNodeDateTime(int level) {
        LocalDateTime dateTime = LocalDateTime.of(0, 1, 1, 0, 0, 0, 0);
        for (int i = 1; i <= level; i++) {
            dateTime = dateTime.with(TEMPORAL_HIERARCHY.get(i - 1), stack.get(i).getLabel());
        }
        return dateTime;
    }

    private List<Integer> getLabels(LocalDateTime date) {
        List<Integer> labels = new ArrayList<>();
        for (TemporalField temporalField : TEMPORAL_HIERARCHY) {
            labels.add(date.get(temporalField));
        }
        return labels;
    }

}
