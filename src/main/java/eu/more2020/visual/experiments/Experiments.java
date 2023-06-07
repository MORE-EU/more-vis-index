package eu.more2020.visual.experiments;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import eu.more2020.visual.domain.*;
import eu.more2020.visual.domain.Dataset.*;
import eu.more2020.visual.domain.InfluxDB.InfluxDBConnection;
import eu.more2020.visual.domain.Query.*;
import eu.more2020.visual.domain.QueryExecutor.QueryExecutor;
import eu.more2020.visual.domain.QueryExecutor.QueryExecutorFactory;
import eu.more2020.visual.experiments.util.*;
import eu.more2020.visual.index.RawTTI;
import eu.more2020.visual.index.TTI;
import eu.more2020.visual.domain.QueryExecutor.InfluxDBQueryExecutor;
import eu.more2020.visual.domain.Detection.PostgreSQL.PostgreSQLConnection;
import eu.more2020.visual.domain.QueryExecutor.SQLQueryExecutor;
import eu.more2020.visual.util.io.SerializationUtilities;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.math3.analysis.function.Abs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.text.View;
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

    @Parameter(names = "-timeCol", description = "Datetime Column for CSV files")
    public String timeCol;

    @Parameter(names = "-hasHeader", description = "If CSV has header")
    public Boolean hasHeader = true;

    @Parameter(names = "-timeFormat", description = "Datetime Column Format")
    public String timeFormat = "yyyy-MM-dd[ HH:mm:ss]";

    @Parameter(names = "-delimeter", description = "CSV Delimeter")
    public String delimiter = ",";


    @Parameter(names = "-zoomFactor", description = "Zoom factor for zoom in operation. The inverse applies to zoom out operation.")
    public Float zoomFactor = 0f;


    @Parameter(names = "-startTime", converter = EpochConverter.class,  variableArity = true, description = "Start Time Epoch")
    Long startTime = 0L;

    @Parameter(names = "-endTime", converter = EpochConverter.class,  variableArity = true, description = "End Time Epoch")
    Long endTime = 0L;

    @Parameter(names = "-filters", converter = FilterConverter.class, description = "Q0 Filters")
    private HashMap<Integer, Double[]> filters = new HashMap<>();


    @Parameter(names = "-c", required = true)
    private String command;

    @Parameter(names = "-a")
    private float accuracy;

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
    @Parameter(names = "--measureMem", description = "Measure index memory after every query in the sequence")
    private boolean measureMem = false;

    @Parameter(names = "--groupBy", converter = OLAPConverter.class,  description = "Measure index memory after every query in the sequence")
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
        Preconditions.checkNotNull(outFolder,"No out folder specified.");
        type = type.toLowerCase(Locale.ROOT);
        initOutput();
        switch (command) {
            case "initialize":
                initialize();
                break;
            case "timeQueries":
                timeQueries();
                break;
            case "plot":
                plotQuery();
                break;
            default:
        }
    }

    private void initializePostgreSQL()throws IOException, SQLException{
        AbstractDataset dataset = createDataset();
        PostgreSQLConnection postgreSQLConnection = new PostgreSQLConnection(config);
        SQLQueryExecutor sqlQueryExecutor = postgreSQLConnection.getSqlQueryExecutor(dataset.getSchema(), dataset.getName());
        sqlQueryExecutor.drop();
        sqlQueryExecutor.initialize(path);
    }

    private void initializeInfluxDB()throws IOException, SQLException{
        AbstractDataset dataset = createDataset();
        InfluxDBConnection influxDBConnection = new InfluxDBConnection(config);
        InfluxDBQueryExecutor influxDBQueryExecutor = influxDBConnection.getSqlQueryExecutor(dataset.getSchema(),
                dataset.getName(), dataset.getHeader());
        influxDBQueryExecutor.drop();
        influxDBQueryExecutor.initialize(path);
    }

    private void initialize() throws IOException, SQLException {
        Preconditions.checkNotNull(mode, "You must define the execution mode (tti, raw, postgres, influx).");
        switch(mode) {
            case "postgres":
                initializePostgreSQL();
            case "influx":
                initializeInfluxDB();
            default:
                System.exit(0);
        }
    }

    private void timeQueriesTTI() throws IOException, SQLException {
        String resultsPath = Paths.get(outFolder,"timeQueries", type, table, "ttiResults").toString();
        File outFile = Paths.get(resultsPath, "results.csv").toFile();
        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        CsvWriter csvWriter = new CsvWriter(new FileWriter(outFile, false), csvWriterSettings);
        Stopwatch stopwatch = Stopwatch.createUnstarted();
        AbstractDataset dataset = createDataset();
        TTI tti = new TTI(dataset);
        QueryMethod queryMethod = QueryMethod.M4_MULTI;
        Query q0 = new Query(startTime, endTime, accuracy, queryMethod, measures, new ViewPort(1000, 600));
        List<AbstractQuery> sequence = generateQuerySequence(q0, dataset);
        csvWriter.writeHeaders("dataset","query #", "operation", "timeRange", "Results size", "IO Count", "Time (sec)", "Memory",  "Error");
        for (int i = 0; i < sequence.size(); i += 1) {
            stopwatch.start();
            Query query = (Query) sequence.get(i);
            QueryResults queryResults;
            double time = 0;
            LOG.info("Executing query " + i  +  " " + query.getFromDate() + " - " + query.getToDate());
            queryResults = tti.executeQuery(query);
            time = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
            long memorySize = tti.calculateDeepMemorySize();
            queryResults.toMultipleCsv(Paths.get(resultsPath, "query_" + i).toString());
            csvWriter.addValue(table);
            csvWriter.addValue(i);
            csvWriter.addValue(query.getOpType());
            csvWriter.addValue(query.getFromDate() + " - " + query.getToDate());
            csvWriter.addValue(0);
            csvWriter.addValue(queryResults.getIoCount());
            csvWriter.addValue(time);
            csvWriter.addValue(memorySize);
            csvWriter.addValue(queryResults.getError());
            csvWriter.writeValuesToRow();
            System.out.println();
            stopwatch.reset();
        }
        csvWriter.flush();
    }

    private void timeQueriesRawTTI() throws IOException, SQLException {
        String resultsPath = Paths.get(outFolder,"timeQueries", type, table, "rawResults").toString();
        File outFile = Paths.get(resultsPath, "results.csv").toFile();
        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        CsvWriter csvWriter = new CsvWriter(new FileWriter(outFile, false), csvWriterSettings);
        Stopwatch stopwatch = Stopwatch.createUnstarted();
        AbstractDataset dataset = createDataset();
        RawTTI rawTTI = new RawTTI(dataset);
        QueryMethod queryMethod = QueryMethod.M4_MULTI;
        Query q0 = new Query(startTime, endTime, accuracy, queryMethod, measures, new ViewPort(1000, 600));
        List<AbstractQuery> sequence = generateQuerySequence(q0, dataset);
        csvWriter.writeHeaders("dataset","query #", "operation", "timeRange", "Results size", "IO Count", "Time (sec)", "Memory");
        for (int i = 0; i < sequence.size(); i += 1) {
            stopwatch.start();
            Query query = (Query) sequence.get(i);
            QueryResults queryResults;
            double time = 0;
            LOG.info("Executing query " + i  +  " " + query.getFromDate() + " - " + query.getToDate());
            queryResults = rawTTI.executeQuery(query);
            time = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
            long memorySize = rawTTI.calculateDeepMemorySize();
            queryResults.toMultipleCsv(Paths.get(resultsPath, "query_" + i).toString());
            csvWriter.addValue(table);
            csvWriter.addValue(i);
            csvWriter.addValue(query.getOpType());
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

    private void timeQueriesM4() throws IOException, SQLException {
        String resultsPath = Paths.get(outFolder,"timeQueries", type, table, "m4Results").toString();
        File outFile = Paths.get(resultsPath, "results.csv").toFile();
        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        CsvWriter csvWriter = new CsvWriter(new FileWriter(outFile, false), csvWriterSettings);
        Stopwatch stopwatch = Stopwatch.createUnstarted();
        AbstractDataset dataset = createDataset();
        QueryExecutor queryExecutor = QueryExecutorFactory.getQueryExecutor(dataset);
        QueryMethod queryMethod = QueryMethod.M4;
        AbstractQuery q0 = generateFirstQuery(dataset);
        List<AbstractQuery> sequence = generateQuerySequence(q0, dataset);
        csvWriter.writeHeaders("dataset","query #", "operation", "timeRange", "Results size", "Time (sec)");
        for (int i = 0; i < sequence.size(); i += 1) {
            stopwatch.start();
            AbstractQuery query = sequence.get(i);
            QueryResults queryResults;
            double time = 0;
            LOG.info("Executing query " + i  +  " " + query.getFromDate() + " - " + query.getToDate());
            queryResults = queryExecutor.execute(query, queryMethod);
            time = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
            queryResults.toMultipleCsv(Paths.get(resultsPath, "query_" + i).toString());
            csvWriter.addValue(table);
            csvWriter.addValue(i);
            csvWriter.addValue(query.getOpType());
            csvWriter.addValue(query.getFromDate() + " - " + query.getToDate());
            csvWriter.addValue(queryResults.getData().get(this.measures.get(0)).size());
            csvWriter.addValue(time);
            csvWriter.writeValuesToRow();
            System.out.println();
            stopwatch.reset();
        }
        csvWriter.flush();
    }

    private void timeQueries() throws IOException, SQLException {
        Preconditions.checkNotNull(mode, "You must define the execution mode (tti, raw, postgres, influx).");
        Path path = Paths.get(outFolder,"timeQueries", type, table, mode + "Results");
        FileUtil.build(path.toString());
        switch(mode) {
            case "tti":
                timeQueriesTTI();
                break;
            case "raw":
                timeQueriesRawTTI();
                break;
            case "m4":
                timeQueriesM4();
                break;
            default:
                System.exit(0);
        }
    }

    private AbstractQuery generateFirstQuery(AbstractDataset dataset){
        switch (type) {
            case "postgres":
                return new SQLQuery(startTime, endTime, measures, new ViewPort(1000, 600));
            case "influx":
                List<String> measureNames = measures.stream().map(m -> dataset.getHeader()[m]).collect(Collectors.toList());
                return new InfluxDBQuery(startTime, endTime, measureNames, new ViewPort(1000, 600));
            default:
                LOG.error("Wrong type of dataset provided");
                System.exit(0);
        }
        return null;
    }

    private List<AbstractQuery> generateQuerySequence(AbstractQuery q0, AbstractDataset dataset) {
        Preconditions.checkNotNull(seqCount, "No sequence count specified.");
        Preconditions.checkNotNull(minShift, "Min query shift must be specified.");
        Preconditions.checkNotNull(maxShift, "Max query shift must be specified.");
        QuerySequenceGenerator sequenceGenerator = new QuerySequenceGenerator(minShift, maxShift, zoomFactor, dataset);
        return sequenceGenerator.generateQuerySequence(q0, seqCount);
    }


    private void plotQuery() throws IOException, SQLException {
        String plotFolder = "plotQuery";
        recreateDir(Paths.get(outFolder, plotFolder).toString());
        String rawTTiResultsPath = Paths.get(outFolder,"plotQuery", "rawResults").toString();
        String ttiResultsPath = Paths.get(outFolder, "plotQuery", "ttiResults").toString();
        String sqlResultsPath = Paths.get(outFolder, "plotQuery", "sqlResults").toString();
        String influxDBResultsPath = Paths.get(outFolder, "plotQuery", "influxDBResults").toString();
        ViewPort viewPort = new ViewPort(1000, 800);
        AbstractDataset dataset = createDataset();
        Query ttiQuery = new Query(startTime, endTime, measures, viewPort, groupyBy);
        List<String> measureNames = ttiQuery.getMeasures().stream().map(m -> dataset.getHeader()[m]).collect(Collectors.toList());
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
        Path metadataPath = Paths.get(outFolder, "metadata");

        FileUtil.build(outFolderPath.toString());
        FileUtil.build(timeQueriesPath.toString());
        FileUtil.build(typePath.toString());
        FileUtil.build(tablePath.toString());
        FileUtil.build(metadataPath.toString());
//        recreateDir(outFolder);
    }

    private AbstractDataset createDataset() throws IOException, SQLException {
        String p = "";
        switch (type) {
            case "csv":
                p = String.valueOf(Paths.get(outFolder, "metadata", "csv-" + table));
                if(new File(p).exists()) return (CsvDataset) SerializationUtilities.loadSerializedObject(p);
                CsvDataset csvDataset = new CsvDataset(path, "0", "test", timeCol, hasHeader, timeFormat, delimiter);
                SerializationUtilities.storeSerializedObject(csvDataset, p);
                return csvDataset;
            case "parquet":
                p =  String.valueOf(Paths.get(outFolder, "metadata",  "parquet-" + table));
                if(new File(p).exists()) return (ParquetDataset) SerializationUtilities.loadSerializedObject(p);
                ParquetDataset parquetDataset = new ParquetDataset(path, "0", "test", timeCol, timeFormat);
                SerializationUtilities.storeSerializedObject(parquetDataset, p);
                return parquetDataset;
            case "postgres":
                p = String.valueOf(Paths.get(outFolder, "metadata", "postgres-" +table));
                if(new File(p).exists()) return (PostgreSQLDataset) SerializationUtilities.loadSerializedObject(p);
                PostgreSQLDataset postgreSQLDataset = new PostgreSQLDataset(config, schema, table, timeFormat);
                SerializationUtilities.storeSerializedObject(postgreSQLDataset, p);
                return postgreSQLDataset;
            case "influx":
                p = String.valueOf(Paths.get(outFolder, "metadata", "influx-" + table));
                if(new File(p).exists()) return (InfluxDBDataset) SerializationUtilities.loadSerializedObject(p);
                InfluxDBDataset influxDBDataset = new InfluxDBDataset(config, schema, table, timeFormat);
                SerializationUtilities.storeSerializedObject(influxDBDataset, p);
                return influxDBDataset;
            default:
                break;
        }
        return null;
    }

}
