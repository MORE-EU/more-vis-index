package eu.more2020.visual.index.csv;

import com.univocity.parsers.csv.CsvParserSettings;
import eu.more2020.visual.domain.*;
import eu.more2020.visual.domain.Dataset.CsvDataset;
import eu.more2020.visual.index.TreeNode;
import eu.more2020.visual.util.io.CsvReader.CsvRandomAccessReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.*;

public class CsvTTI {

    private static final Logger LOG = LogManager.getLogger(CsvTTI.class);
    protected CsvTreeNode root;
    private CsvRandomAccessReader csvRandomAccessReader;
    private Map<Integer, DoubleSummaryStatistics> measureStats;
    private CsvDataset dataset;
    private String csv;
    private int objectsIndexed = 0;
    private boolean isInitialized = false;
    private DateTimeFormatter formatter;


    public CsvTTI(String csv, CsvDataset dataset) {
        this.dataset = dataset;
        this.csv = csv;
        this.formatter =
            new DateTimeFormatterBuilder().appendPattern(dataset.getTimeFormat())
                .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
                .toFormatter();

    }

    private TreeNode addPoint(Stack<Integer> labels, long fileOffset, String[] row) {
        if (root == null) {
            root = new CsvTreeNode(0, 0);
        }
        // adjust root node meta
        if (root.getDataPointCount() == 0) {
            root.setFileOffsetStart(fileOffset);
        }
        root.setDataPointCount(root.getDataPointCount() + 1);
        root.adjustStats(row, dataset);

        CsvTreeNode node = root;
        if (node.getDataPointCount() == 0) {
            node.setFileOffsetStart(fileOffset);
        }
        node.setDataPointCount(node.getDataPointCount() + 1);
        node.adjustStats(row, dataset);

        for (Integer label : labels) {
            CsvTreeNode child = (CsvTreeNode) node.getOrAddChild(label);
            node = child;
            if (node.getDataPointCount() == 0) {
                node.setFileOffsetStart(fileOffset);
            }
            node.setDataPointCount(node.getDataPointCount() + 1);
            node.adjustStats(row, dataset);
        }
        return node;
    }



    public void initialize() throws IOException {
        measureStats = new HashMap<>();
        List<Integer> measures = this.dataset.getMeasures();
        for (Integer measureIndex : measures) {
            measureStats.put(measureIndex, new DoubleSummaryStatistics());
        }
        objectsIndexed = 0;
        this.csvRandomAccessReader = new CsvRandomAccessReader(csv, formatter,
            dataset.getTimeCol(), dataset.getDelimiter(), dataset.getHasHeader(), dataset.getMeasures());
        this.dataset.setSamplingFreq(this.csvRandomAccessReader.sample());
    }

    public synchronized QueryResults executeQuery(Query query) throws IOException {
        if (!isInitialized) {
            initialize();
        }
        CsvQueryProcessor queryProcessor = new CsvQueryProcessor(query, dataset, this);
        return queryProcessor.prepareQueryResults(root, query.getFilter());
    }

    public void traverse(TreeNode node) {
        LOG.debug(node.toString());
        Collection<TreeNode> children = node.getChildren();
        if (children != null && !children.isEmpty()) {
            for (TreeNode child : children) {
                traverse(child);
            }
        }
    }

    public LocalDateTime parseStringToDate(String s) { return this.csvRandomAccessReader.parseStringToDate(s);}

    public String getCsv() {
        return csv;
    }

    public Map<Integer, DoubleSummaryStatistics> getMeasureStats() {
        return measureStats;
    }

    public DateTimeFormatter getFormatter(){
        return this.formatter;
    }

    public String[] testRandomAccess (LocalDateTime time, List<Integer> measures) throws IOException {
        return this.csvRandomAccessReader.getData(time, measures);
    }

    public ArrayList<String[]> testRandomAccessRange (TimeRange range,  List<Integer> measures) throws IOException {
        return this.csvRandomAccessReader.getData(range, measures);
    }

    public CsvParserSettings getCsvParserSettings(){
        return this.csvRandomAccessReader.getCsvParserSettings();
    }

}
