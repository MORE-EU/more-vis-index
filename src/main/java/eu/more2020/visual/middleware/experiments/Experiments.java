package eu.more2020.visual.middleware.experiments;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import eu.more2020.visual.middleware.cache.MinMaxCache;
import eu.more2020.visual.middleware.datasource.DataSourceQuery;
import eu.more2020.visual.middleware.datasource.InfluxDBQuery;
import eu.more2020.visual.middleware.datasource.ModelarDBQuery;
import eu.more2020.visual.middleware.datasource.QueryExecutor.InfluxDBQueryExecutor;
import eu.more2020.visual.middleware.datasource.QueryExecutor.QueryExecutor;
import eu.more2020.visual.middleware.datasource.QueryExecutor.QueryExecutorFactory;
import eu.more2020.visual.middleware.datasource.QueryExecutor.SQLQueryExecutor;
import eu.more2020.visual.middleware.datasource.SQLQuery;
import eu.more2020.visual.middleware.domain.Dataset.*;
import eu.more2020.visual.middleware.domain.ModelarDB.ModelarDBConnection;
import eu.more2020.visual.middleware.domain.PostgreSQL.JDBCConnection;
import eu.more2020.visual.middleware.domain.InfluxDB.InfluxDBConnection;
import eu.more2020.visual.middleware.domain.Query.Query;
import eu.more2020.visual.middleware.domain.Query.QueryMethod;
import eu.more2020.visual.middleware.domain.QueryResults;
import eu.more2020.visual.middleware.domain.ViewPort;
import eu.more2020.visual.middleware.experiments.util.*;
import eu.more2020.visual.middleware.cache._MinMaxCache;
import eu.more2020.visual.middleware.cache.RawCache;
import eu.more2020.visual.middleware.util.io.SerializationUtilities;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class Experiments<T> {
    private static final Logger LOG = LoggerFactory.getLogger(Experiments.class);

    @Parameter(names = "-path", description = "The path of the input file(s)")
    public String path;

    @Parameter(names = "-type", description = "The type of the input")
    public String type;

    @Parameter(names = "-mode", description = "The mode of the experiment (tti/raw/influx/postgres")
    public String mode;

    @Parameter(names = "-measures", variableArity = true, description = "Measures IDs to be used")
    public List<Integer> measures;

    @Parameter(names = "-measureNames", variableArity = true, description = "Measures Names to be used")
    public List<String> measureNames;

    @Parameter(names = "-timeCol", description = "Datetime Column name")
    public String timeCol;

    @Parameter(names = "-idCol", description = "Measure name/id column name")
    public String idCol;

    @Parameter(names = "-valueCol", description = "Value Column name")
    public String valueCol;

    @Parameter(names = "-hasHeader", description = "If CSV has header")
    public Boolean hasHeader = true;

    @Parameter(names = "-timeFormat", description = "Datetime Column Format")
    public String timeFormat = "yyyy-MM-dd[ HH:mm:ss]";

    @Parameter(names = "-delimeter", description = "CSV Delimeter")
    public String delimiter = ",";


    @Parameter(names = "-zoomFactor", description = "Zoom factor for zoom in operation. The inverse applies to zoom out operation.")
    public Float zoomFactor = 0f;


    @Parameter(names = "-startTime", converter = EpochConverter.class, variableArity = true, description = "Start Time Epoch")
    Long startTime = 0L;

    @Parameter(names = "-endTime", converter = EpochConverter.class, variableArity = true, description = "End Time Epoch")
    Long endTime = 0L;

    @Parameter(names = "-q", description = "Query percent")
    Double q;

    @Parameter(names = "-p", description = "Prefetching factor")
    Double p;

    @Parameter(names = "-filters", converter = FilterConverter.class, description = "Q0 Filters")
    private HashMap<Integer, Double[]> filters = new HashMap<>();


    @Parameter(names = "-c", required = true)
    private String command;

    @Parameter(names = "-a")
    private float accuracy;

    @Parameter(names = "-agg")
    private int aggFactor;

    @Parameter(names = "-reduction")
    private int reductionFactor;

    @Parameter(names = "-out", description = "The output folder")
    private String outFolder;

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
    @Parameter(names = "-config", description = "PostgreSQL/InfluxDB config file path")
    private String config;
    @Parameter(names = "-schema", description = "PostgreSQL/InfluxDB schema name where data lay")
    private String schema;
    @Parameter(names = "-table", description = "PostgreSQL/InfluxDB table name to query")
    private String table;
    @Parameter(names = "-viewport", converter = ViewPortConverter.class, description = "Viewport of query")
    private ViewPort viewPort;
    @Parameter(names = "-runs", description = "Times to run each experiment workflow")
    private Integer runs;
    @Parameter(names = "--measureMem",  description = "Measure index memory after every query in the sequence")
    private boolean measureMem = false;


    @Parameter(names = "-org", description = "The organization for InfluxDB")
    public String org;

    @Parameter(names = "--groupBy", converter = OLAPConverter.class, description = "Measure index memory after every query in the sequence")
    private ChronoField groupyBy = ChronoField.HOUR_OF_DAY;

    @Parameter(names = "--help", help = true, description = "Displays help")


    private boolean help;


    private final Properties influxDbProperties = new Properties();

    public Experiments() {
    }


    public static void main(String... args) throws IOException, ClassNotFoundException, SQLException {
        Experiments experiments = new Experiments();
        JCommander jCommander = new JCommander(experiments, args);
        if (experiments.help) {
            jCommander.usage();
        } else {
            experiments.run();
        }
    }

    private void run() throws IOException, ClassNotFoundException, SQLException {
        Preconditions.checkNotNull(outFolder, "No out folder specified.");
        type = type.toLowerCase(Locale.ROOT);
        switch(type){
            case "postgres":
                if(config == null) config = "postgreSQL.cfg";
                break;
            case "influx":
                if(config == null) config = "influxDB.cfg";
                break;
            case "modelar":
                if(config == null) config = "modelarDB.cfg";
                break;
            default:
                Preconditions.checkNotNull(outFolder, "No config files specified.");
        }
        initOutput();
        switch (command) {
            case "initialize":
                initialize();
                break;
            case "timeQueries":
                timeQueries();
                break;
            default:
        }
    }

    private void initializePostgreSQL() throws  SQLException {
        JDBCConnection postgreSQLConnection = new JDBCConnection(config);
        SQLQueryExecutor sqlQueryExecutor = postgreSQLConnection.getQueryExecutor();
        sqlQueryExecutor.drop();
        sqlQueryExecutor.initialize(path);
    }

    private void initializeInfluxDB() throws IOException {
        InfluxDBConnection influxDBConnection = new InfluxDBConnection(config);
        InfluxDBQueryExecutor influxDBQueryExecutor = influxDBConnection.getQueryExecutor();
        influxDBQueryExecutor.drop();
        influxDBQueryExecutor.initialize(path);
    }

    private void initialize() throws IOException, SQLException {
        Preconditions.checkNotNull(type, "You must define the execution type (postgres, influx).");
        switch (type) {
            case "postgres":
                initializePostgreSQL();
            case "influx":
                initializeInfluxDB();
            default:
                System.exit(0);
        }
    }

    private void timeQueriesTTIM4(int run) throws IOException, SQLException {
//        String resultsPath = Paths.get(outFolder, "timeQueries", type, table, "run_" + run, "ttiM4Results").toString();
//        File outFile = Paths.get(resultsPath, "results.csv").toFile();
//        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
//        CsvWriter csvWriter = new CsvWriter(new FileWriter(outFile, false), csvWriterSettings);
//        Stopwatch stopwatch = Stopwatch.createUnstarted();
//        AbstractDataset dataset = createDataset();
//        TTI tti = new TTI(dataset, p, aggFactor, reductionFactor);
//        QueryMethod queryMethod = QueryMethod.M4_MULTI;
//        Query q0 = new Query(startTime, endTime, accuracy, null, queryMethod, measures, viewPort, null);
//        List<Query> sequence = generateQuerySequence(q0, dataset);
//        csvWriter.writeHeaders("dataset", "query #", "operation", "width", "height", "from", "to", "timeRange", "aggFactor", "Results size", "IO Count",
//                "Time (sec)", "Processing Time", "Query Time", "Memory", "Error", "flag");
//        for (int i = 0; i < sequence.size(); i += 1) {
//            stopwatch.start();
//            Query query = (Query) sequence.get(i);
//            QueryResults queryResults;
//            double time = 0;
//            LOG.info("Executing query " + i + " " + query.getFromDate() + " - " + query.getToDate());
//            queryResults = tti.executeQueryM4(query);
//            time = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
//            long memorySize = tti.calculateDeepMemorySize();
//            if(run == 0) queryResults.toMultipleCsv(Paths.get(resultsPath, "query_" + i).toString());
//            csvWriter.addValue(table);
//            csvWriter.addValue(i);
//            csvWriter.addValue(query.getOpType());
//            csvWriter.addValue(viewPort.getWidth());
//            csvWriter.addValue(viewPort.getHeight());
//            csvWriter.addValue(query.getFrom());
//            csvWriter.addValue(query.getTo());
//            csvWriter.addValue(query.getFromDate() + " - " + query.getToDate());
//            csvWriter.addValue(queryResults.getAggFactor());
//            csvWriter.addValue(0);
//            csvWriter.addValue(queryResults.getIoCount());
//            csvWriter.addValue(time);
//            csvWriter.addValue(time - queryResults.getQueryTime());
//            csvWriter.addValue(queryResults.getQueryTime());
//            csvWriter.addValue(memorySize);
//            csvWriter.addValue(queryResults.getError());
//            csvWriter.addValue(queryResults.isFlag());
//            csvWriter.writeValuesToRow();
//            System.out.println();
//            stopwatch.reset();
//        }
//        csvWriter.flush();
    }


    private void timeQueriesTTIMinMax(int run) throws IOException, SQLException {
        String resultsPath = Paths.get(outFolder, "timeQueries", type, table, "run_" + run, "ttiMinMaxResults").toString();
        File outFile = Paths.get(resultsPath, "results.csv").toFile();
        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        CsvWriter csvWriter = new CsvWriter(new FileWriter(outFile, false), csvWriterSettings);
        Stopwatch stopwatch = Stopwatch.createUnstarted();
        AbstractDataset dataset = createDataset();
        QueryExecutor queryExecutor = createQueryExecutor(dataset);
        MinMaxCache minMaxCache = new MinMaxCache(queryExecutor, dataset, p, aggFactor, reductionFactor);
        QueryMethod queryMethod = QueryMethod.MIN_MAX;
        Query q0 = new Query(startTime, endTime, accuracy, null, queryMethod, measures, viewPort, null);
        List<Query> sequence = generateQuerySequence(q0, dataset);
        csvWriter.writeHeaders("dataset", "query #", "operation", "width", "height", "from", "to", "timeRange", "aggFactor", "Results size", "IO Count",
                "Time (sec)", "Progressive Time (sec)", "Processing Time (sec)", "Query Time (sec)", "Memory", "Error", "flag");
        for (int i = 0; i < sequence.size(); i += 1) {
            stopwatch.start();
            Query query = (Query) sequence.get(i);
            QueryResults queryResults;
            double time = 0;
            LOG.info("Executing query " + i + " " + query.getOpType() + " " + query.getFromDate() + " - " + query.getToDate());
            queryResults = minMaxCache.executeQuery(query);
            time = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
            long memorySize = minMaxCache.calculateDeepMemorySize();
            if(run == 0) queryResults.toMultipleCsv(Paths.get(resultsPath, "query_" + i).toString());
            csvWriter.addValue(table);
            csvWriter.addValue(i);
            csvWriter.addValue(query.getOpType());
            csvWriter.addValue(viewPort.getWidth());
            csvWriter.addValue(viewPort.getHeight());
            csvWriter.addValue(query.getFrom());
            csvWriter.addValue(query.getTo());
            csvWriter.addValue(query.getFromDate() + " - " + query.getToDate());
            csvWriter.addValue(queryResults.getAggFactor());
            csvWriter.addValue(0);
            csvWriter.addValue(queryResults.getIoCount());
            csvWriter.addValue(time);
            csvWriter.addValue(queryResults.getProgressiveQueryTime());
            csvWriter.addValue(time - queryResults.getQueryTime());
            csvWriter.addValue(queryResults.getQueryTime());
            csvWriter.addValue(memorySize);
            csvWriter.addValue(queryResults.getError());
            csvWriter.addValue(queryResults.isFlag());
            csvWriter.writeValuesToRow();
            System.out.println();
            stopwatch.reset();
        }
        csvWriter.flush();
    }

    private void timeQueriesRawTTI(int run) throws IOException, SQLException {
        String resultsPath = Paths.get(outFolder, "timeQueries", type, table, "run_" + run, "rawResults").toString();
        File outFile = Paths.get(resultsPath, "results.csv").toFile();
        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        CsvWriter csvWriter = new CsvWriter(new FileWriter(outFile, false), csvWriterSettings);
        Stopwatch stopwatch = Stopwatch.createUnstarted();
        AbstractDataset dataset = createDataset();
        QueryExecutor queryExecutor = createQueryExecutor(dataset);
        RawCache rawCache = new RawCache(queryExecutor, dataset);
        QueryMethod queryMethod = QueryMethod.RAW;
        Query q0 = new Query(startTime, endTime, accuracy, null, queryMethod, measures, viewPort, null);
        List<Query> sequence = generateQuerySequence(q0, dataset);
        csvWriter.writeHeaders("dataset", "query #", "operation", "width", "height", "from", "to", "timeRange", "Results size", "IO Count",  "Time (sec)", "Memory");
        for (int i = 0; i < sequence.size(); i += 1) {
            stopwatch.start();
            Query query = (Query) sequence.get(i);
            QueryResults queryResults;
            double time = 0;
            LOG.info("Executing query " + i + " " + query.getFromDate() + " - " + query.getToDate());
            queryResults = rawCache.executeQuery(query);
            time = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
            long memorySize = rawCache.calculateDeepMemorySize();
            if(run == 0) queryResults.toMultipleCsv(Paths.get(resultsPath, "query_" + i).toString());
            csvWriter.addValue(table);
            csvWriter.addValue(i);
            csvWriter.addValue(query.getOpType());
            csvWriter.addValue(viewPort.getWidth());
            csvWriter.addValue(viewPort.getHeight());
            csvWriter.addValue(query.getFrom());
            csvWriter.addValue(query.getTo());
            csvWriter.addValue(query.getFromDate() + " - " + query.getToDate());
            csvWriter.addValue(queryResults.getData().get(this.measures.get(0)).size());
            csvWriter.addValue(queryResults.getIoCount());
            csvWriter.addValue(time);
            csvWriter.addValue(memorySize);
            csvWriter.writeValuesToRow();
            System.out.println();
            stopwatch.reset();

        }
        csvWriter.flush();
    }

    private void timeQueriesM4(int run) throws IOException, SQLException {
        String resultsPath = Paths.get(outFolder, "timeQueries", type, table, "run_" + run, "m4Results").toString();
        File outFile = Paths.get(resultsPath, "results.csv").toFile();
        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        CsvWriter csvWriter = new CsvWriter(new FileWriter(outFile, false), csvWriterSettings);
        Stopwatch stopwatch = Stopwatch.createUnstarted();
        AbstractDataset dataset = createDataset();
        QueryExecutor queryExecutor = QueryExecutorFactory.getQueryExecutor(dataset);
        QueryMethod queryMethod = QueryMethod.M4;
        Query q0 = new Query(startTime, endTime, accuracy, null, queryMethod, measures, viewPort, null);
        List<Query> sequence = generateQuerySequence(q0, dataset);
        csvWriter.writeHeaders("dataset", "query #", "operation", "width", "height", "from", "to", "timeRange", "Results size", "Time (sec)");
        for (int i = 0; i < sequence.size(); i += 1) {
            stopwatch.start();
            Query query = sequence.get(i);
            QueryResults queryResults;
            double time = 0;
            LOG.info("Executing query " + i + " " + query.getFromDate() + " - " + query.getToDate());
            List<String> measureNames = measures.stream().map(m -> dataset.getHeader()[m]).collect(Collectors.toList());
            DataSourceQuery dataSourceQuery = null;
            switch (type) {
                case "postgres":
                    dataSourceQuery = new SQLQuery(query.getFrom(), query.getTo(), query.getMeasures(), query.getViewPort().getWidth());
                    break;
                case "modelar":
                    dataSourceQuery = new ModelarDBQuery(query.getFrom(), query.getTo(), query.getMeasures(), measureNames, query.getViewPort().getWidth());
                    break;
                case "influx":
                    dataSourceQuery = new InfluxDBQuery(query.getFrom(), query.getTo(), query.getMeasures(), measureNames, query.getViewPort().getWidth());
                    break;
            }
            queryResults = queryExecutor.execute(dataSourceQuery, queryMethod);
            stopwatch.stop();
            time = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
            if(run == 0) queryResults.toMultipleCsv(Paths.get(resultsPath, "query_" + i).toString());
            csvWriter.addValue(table);
            csvWriter.addValue(i);
            csvWriter.addValue(query.getOpType());
            csvWriter.addValue(viewPort.getWidth());
            csvWriter.addValue(viewPort.getHeight());
            csvWriter.addValue(query.getFrom());
            csvWriter.addValue(query.getTo());
            csvWriter.addValue(query.getFromDate() + " - " + query.getToDate());
            csvWriter.addValue(queryResults.getData().get(this.measures.get(0)).size());
            csvWriter.addValue(time);
            csvWriter.writeValuesToRow();
            stopwatch.reset();
        }
        csvWriter.flush();
    }

    private void timeQueries() throws IOException, SQLException {
        Preconditions.checkNotNull(mode, "You must define the execution mode (tti, raw, postgres, influx).");
        for  (int i = 0; i < runs; i ++){
            Path runPath = Paths.get(outFolder, "timeQueries", type, table, "run_" + i);
            FileUtil.build(runPath.toString());
            if(!mode.equals("all")) {
                Path path = Paths.get(outFolder, "timeQueries", type, table, "run_" + i, mode + "Results");
                FileUtil.build(path.toString());
            }
            else {
                Path path1 = Paths.get(outFolder, "timeQueries", type, table, "run_" + i, "ttiMinMax" + "Results");
                Path path2 = Paths.get(outFolder, "timeQueries", type, table, "run_" + i, "m4" + "Results");
                Path path3 = Paths.get(outFolder, "timeQueries", type, table, "run_" + i, "raw" + "Results");
                FileUtil.build(path1.toString());
                FileUtil.build(path2.toString());
                FileUtil.build(path3.toString());
            }
        }
        for(int i = 0; i < runs; i ++) {
            switch (mode) {
                case "ttiM4":
                    timeQueriesTTIM4(i);
                    break;
                case "ttiMinMax":
                    timeQueriesTTIMinMax(i);
                    break;
                case "tti":
                    timeQueriesTTIMinMax(i);
                    break;
                case "raw":
                    timeQueriesRawTTI(i);
                    break;
                case "m4":
                    timeQueriesM4(i);
                    break;
                case "all":
                    timeQueriesTTIMinMax(i);
                    timeQueriesM4(i);
                    timeQueriesRawTTI(i);
                    break;
                default:
                    System.exit(0);
            }
        }
    }


    private List<Query> generateQuerySequence(Query q0, AbstractDataset dataset) {
        Preconditions.checkNotNull(seqCount, "No sequence count specified.");
        Preconditions.checkNotNull(minShift, "Min query shift must be specified.");
        Preconditions.checkNotNull(maxShift, "Max query shift must be specified.");
        QuerySequenceGenerator sequenceGenerator = new QuerySequenceGenerator(minShift, maxShift, zoomFactor, dataset);
        return sequenceGenerator.generateQuerySequence(q0, seqCount);
    }

    private void recreateDir(String folder) {
        try {
            File f = new File(folder);
            if (f.exists()) {
                FileUtils.cleanDirectory(f); //clean out directory (this is optional -- but good know)
                FileUtils.forceDelete(f); //delete directory
            }
            FileUtils.forceMkdir(f); //create directory
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initOutput() throws IOException {
        Path outFolderPath = Paths.get(outFolder);
        Path timeQueriesPath = Paths.get(outFolder, "timeQueries");
        Path typePath = Paths.get(outFolder, "timeQueries", type);
        Path tablePath = Paths.get(outFolder, "timeQueries", type, table);
        Path metadataPath = Paths.get("metadata");
        FileUtil.build(outFolderPath.toString());
        FileUtil.build(timeQueriesPath.toString());
        FileUtil.build(typePath.toString());
        FileUtil.build(tablePath.toString());
        FileUtil.build(metadataPath.toString());
//        recreateDir(outFolder);
    }

    private AbstractDataset createDataset() throws IOException, SQLException {
        String p = "";
        AbstractDataset dataset = null;
        switch (type) {
            case "postgres":
                p = String.valueOf(Paths.get("metadata", "postgres-" + table));
                if (new File(p).exists()) dataset = (PostgreSQLDataset) SerializationUtilities.loadSerializedObject(p);
                else{
                    dataset = new PostgreSQLDataset(config, schema, table, timeFormat, timeCol);
                    SerializationUtilities.storeSerializedObject(dataset, p);
                }
                break;
            case "modelar":
                p = String.valueOf(Paths.get("metadata", "modelarDB-" + table));
                if (new File(p).exists()) dataset = (ModelarDBDataset) SerializationUtilities.loadSerializedObject(p);
                else{
                    dataset = new ModelarDBDataset(config, table, schema, table, timeFormat, timeCol, idCol, valueCol);
                    SerializationUtilities.storeSerializedObject(dataset, p);
                }
                break;
            case "influx":
                p = String.valueOf(Paths.get("metadata", "influx-" + table));
                if (new File(p).exists()) dataset = (InfluxDBDataset) SerializationUtilities.loadSerializedObject(p);
                else {
                    dataset = new InfluxDBDataset(config, table, schema, table, timeFormat, timeCol);
                    SerializationUtilities.storeSerializedObject(dataset, p);
                }
                break;
            default:
                break;
        }
        LOG.info("Initialized Dataset: {}, range {}, header {}, sampling interval {}", dataset.getTable(),
                dataset.getTimeRange(), Arrays.asList(dataset.getHeader()), dataset.getSamplingInterval());
        // If query percent given. Change start and end times based on it
        if(q != null){
            startTime = dataset.getTimeRange().getTo() - (long) (q * (dataset.getTimeRange().getTo() - dataset.getTimeRange().getFrom()));
            endTime = (dataset.getTimeRange().getTo());
        }
        return dataset;
    }


    private QueryExecutor createQueryExecutor(AbstractDataset dataset) throws IOException, SQLException {
        String p = "";
        QueryExecutor queryExecutor = null;
        switch (type) {
            case "postgres":
                JDBCConnection postgreSQLConnection =
                        new JDBCConnection(config);
                queryExecutor = postgreSQLConnection.getQueryExecutor(dataset);
                break;
            case "modelar":
                ModelarDBConnection modelarDBConnection =
                        new ModelarDBConnection(config);
                queryExecutor = modelarDBConnection.getSqlQueryExecutor(dataset);
                break;
            case "influx":
                InfluxDBConnection influxDBConnection =
                        new InfluxDBConnection(config);
                queryExecutor = influxDBConnection.getQueryExecutor(dataset);
            default:
                break;
        }
        return queryExecutor;
    }

}
