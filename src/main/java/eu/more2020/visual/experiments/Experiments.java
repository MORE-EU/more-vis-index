package eu.more2020.visual.experiments;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import eu.more2020.visual.domain.Dataset.AbstractDataset;
import eu.more2020.visual.domain.Dataset.CsvDataset;
import eu.more2020.visual.domain.Dataset.ParquetDataset;
import eu.more2020.visual.domain.Query;
import eu.more2020.visual.domain.QueryResults;
import eu.more2020.visual.domain.ViewPort;
import eu.more2020.visual.experiments.util.FilterConverter;
import eu.more2020.visual.experiments.util.QuerySequenceGenerator;
import eu.more2020.visual.experiments.util.SyntheticDatasetGenerator;
import eu.more2020.visual.index.TTI;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.ehcache.sizeof.SizeOf;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;


public class Experiments<T> {

    private static final Logger LOG = LogManager.getLogger(Experiments.class);

    @Parameter(names = "-path", description = "The path of the input file(s)")
    public String path;

    @Parameter(names = "-type", description = "The type of the input file(s) (parquet/csv)")
    public String type;

    @Parameter(names = "-measures", variableArity = true, description = "Measures IDs to be used")
    public List<Integer> measures;

    @Parameter(names = "-timeCol", description = "Datetime Column for CSV files")
    public Integer timeCol;

    @Parameter(names = "-hasHeader", description = "If CSV has header")
    public Boolean hasHeader = true;

    @Parameter(names = "-timeFormat", description = "Datetime Column Format")
    public String timeFormat = "yyyy-MM-dd[ HH:mm:ss]";

    @Parameter(names = "-delimeter", description = "CSV Delimeter")
    public String delimiter = ",";


    @Parameter(names = "-zoomFactor", description = "Zoom factor for zoom in operation. The inverse applies to zoom out operation.")
    public Float zoomFactor = 0f;


    @Parameter(names = "-startTime", variableArity = true, description = "Start Time Epoch")
    Long startTime = 0L;

    @Parameter(names = "-endTime", variableArity = true, description = "End Time Epoch")
    Long endTime = 0L;

    @Parameter(names = "-filters", converter = FilterConverter.class, description = "Q0 Filters")
    private HashMap<Integer, Double[]> filters = new HashMap<>();


    @Parameter(names = "-c", required = true)
    private String command;

    @Parameter(names = "-out", description = "The output file")
    private String outFile;

    @Parameter(names = "-initMode")
    private String initMode;

    @Parameter(names = "-seqCount", description = "Number of queries in the sequence")
    private Integer seqCount;
    @Parameter(names = "-objCount", description = "Number of objects")
    private Integer objCount;
    @Parameter(names = "-minShift", description = "Min shift in the query sequence")
    private Float minShift;
    @Parameter(names = "-maxShift", description = "Max shift in the query sequence")
    private Float maxShift;
    @Parameter(names = "-minFilters", description = "Min filters in the query sequence")
    private Integer minFilters;
    @Parameter(names = "-maxFilters", description = "Max filters in the query sequence")
    private Integer maxFilters;
    @Parameter(names = "--measureMem", description = "Measure index memory after every query in the sequence")
    private boolean measureMem = false;

    @Parameter(names = "--help", help = true, description = "Displays help")
    private boolean help;


    public Experiments() {
    }


    public static void main(String... args) throws IOException, ClassNotFoundException {
        Experiments experiments = new Experiments();
        JCommander jCommander = new JCommander(experiments, args);
        if (experiments.help) {
            jCommander.usage();
        } else {
            experiments.run();
        }
    }

    private void run() throws IOException, ClassNotFoundException {
        SyntheticDatasetGenerator generator;
        switch (command) {
            case "timeInitialization":
                timeInitialization();
                break;
            case "timeQueries":
                timeQueries();
                break;
            case "synth10":
                generator = new SyntheticDatasetGenerator(100000000, 10, 10, outFile);
                generator.generate();
                break;
            case "synth50":
                List<Integer> catCols = new ArrayList<>();
                for (int i = 10; i < 30; i++) {
                    catCols.add(i);
                }
                generator = new SyntheticDatasetGenerator(100000000, 50, 10, outFile);
                generator.generate();
                break;
            default:
        }
    }

    private void timeInitialization() throws IOException, ClassNotFoundException {
        Preconditions.checkNotNull(path, "You must define the input path.");
        Preconditions.checkNotNull(outFile, "No out file specified.");

        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        boolean addHeader = new File(outFile).length() == 0;

        CsvWriter csvWriter = new CsvWriter(new FileWriter(outFile, true), csvWriterSettings);

        long memorySize = 0;
        SizeOf sizeOf = SizeOf.newInstance();

        Stopwatch stopwatch = Stopwatch.createUnstarted();
        stopwatch.start();
        AbstractDataset dataset = createDataset();
        TTI tti = new TTI(dataset);
        Query q0 = new Query(startTime, endTime, measures, filters, new ViewPort(800, 300));
        tti.initialize(q0);

        try {
            memorySize = sizeOf.deepSizeOf(tti);
        } catch (Exception e) {
        }

//        if (addHeader) {
//            csvWriter.writeHeaders("csv", "initMode", "initCatBudget (Gb)", "initCatBudget (nodes)", "Tree Node Count", "q0", "categoricalColumns", "Time (sec)", "Total Util", "Leaf tiles", "Memory (Gb)");
//        }

//        csvWriter.addValue(csv);
//        csvWriter.addValue(initMode);
//        csvWriter.addValue(catBudget);
//        csvWriter.addValue(categoricalNodeBudget);
//        csvWriter.addValue(TreeNode.getInstanceCount());
//        csvWriter.addValue(q0);
//        csvWriter.addValue(schema.getCategoricalColumns());
//        csvWriter.addValue(stopwatch.elapsed(TimeUnit.SECONDS));
//        csvWriter.addValue(veti.getTotalUtil());
//        csvWriter.addValue(leafTiles);
//        csvWriter.addValue((double) memorySize / 1000000000d);
//        csvWriter.writeValuesToRow();
//        csvWriter.close();
    }

    private void timeQueries() throws IOException, ClassNotFoundException {
        Preconditions.checkNotNull(path, "You must define the input path.");
        Preconditions.checkNotNull(outFile, "No out file specified.");

        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        boolean addHeader = new File(outFile).length() == 0;

        CsvWriter csvWriter = new CsvWriter(new FileWriter(outFile, true), csvWriterSettings);

        long memorySize = 0;
        SizeOf sizeOf = SizeOf.newInstance();

        Stopwatch stopwatch = Stopwatch.createUnstarted();
        stopwatch.start();
        AbstractDataset dataset = createDataset();
        TTI tti = new TTI(dataset);
        Query q0 = new Query(startTime, endTime, measures, filters, new ViewPort(800, 300));
        tti.initialize(q0);

        try {
            memorySize = sizeOf.deepSizeOf(tti);
        } catch (Exception e) {
        }
        List<Query> sequence = generateQuerySequence(q0, dataset);
        for (int i = 0; i < sequence.size(); i++) {
            Query query = sequence.get(i);
            LOG.debug("Executing query " + i);

            stopwatch = Stopwatch.createStarted();
            QueryResults queryResults = tti.executeQuery(query);
            stopwatch.stop();

//            csvWriter.addValue(csv);
//            csvWriter.addValue(schema.getCategoricalColumns());
//            csvWriter.addValue(initMode);
//            csvWriter.addValue(catBudget);
//            csvWriter.addValue(categoricalNodeBudget);
//            csvWriter.addValue(binCount);
//            csvWriter.addValue(i);
//            csvWriter.addValue(queryResults.getQuery());
//            csvWriter.addValue(veti.getTotalUtil());
//            csvWriter.addValue(TreeNode.getInstanceCount());
//            csvWriter.addValue(veti.getLeafTileCount());
//            csvWriter.addValue(queryResults.getTileCount());
//            csvWriter.addValue(queryResults.getFullyContainedTileCount());
//            csvWriter.addValue(queryResults.getExpandedNodeCount());
//            csvWriter.addValue(queryResults.getIoCount());
//            csvWriter.addValue(stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9));
//            csvWriter.addValue(queryResults.getStats().getGroupStats().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().xStats(), (oldValue, newValue) -> oldValue)));
//            csvWriter.writeValuesToRow();
        }
        csvWriter.close();
    }



    private List<Query> generateQuerySequence(Query q0, AbstractDataset dataset) {
        Preconditions.checkNotNull(seqCount, "No sequence count specified.");
        Preconditions.checkNotNull(minShift, "Min query shift must be specified.");
        Preconditions.checkNotNull(maxShift, "Max query shift must be specified.");
        Preconditions.checkNotNull(minFilters, "Min filters must be specified.");
        Preconditions.checkNotNull(maxFilters, "Max filters must be specified.");

        QuerySequenceGenerator sequenceGenerator = new QuerySequenceGenerator(minShift, maxShift, minFilters, maxFilters, zoomFactor, dataset);
        return sequenceGenerator.generateQuerySequence(q0, seqCount);
    }

    private AbstractDataset createDataset() throws IOException {
        switch (type.toLowerCase(Locale.ROOT)) {
            case "csv":
                return new CsvDataset(path, "0", "test", timeCol, measures, hasHeader, timeFormat, delimiter);
            case "parquet":
                return new ParquetDataset(path, "0", "test", timeCol, measures, timeFormat);
            default:
                break;
        }
        return null;
    }

}
