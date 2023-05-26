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
import eu.more2020.visual.experiments.util.*;
import eu.more2020.visual.index.RawTTI;
import eu.more2020.visual.index.TTI;
import eu.more2020.visual.domain.QueryExecutor.InfluxDBQueryExecutor;
import eu.more2020.visual.domain.Detection.PostgreSQL.PostgreSQLConnection;
import eu.more2020.visual.domain.QueryExecutor.SQLQueryExecutor;
import eu.more2020.visual.util.io.SerializationUtilities;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SerializationUtils;
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

    @Parameter(names = "-type", description = "The type of the input file(s) (parquet/csv)")
    public String type;

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
    @Parameter(names = "-postgreSQLCfg", description = "PostgreSQL config file path")
    private String postgreSQLCfg;
    @Parameter(names = "-influxDBCfg", description = "InfluxDB config file path")
    private String influxDBCfg;
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
        Preconditions.checkNotNull(path, "You must define the input path.");
        Preconditions.checkNotNull(outFolder,"No out folder specified.");
        type = type.toLowerCase(Locale.ROOT);
        initOutput();
        switch (command) {
            case "timeInitialization":
                timeInitialization();
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

    private void timeInitialization() throws IOException, ClassNotFoundException, SQLException {

        double ttiTime = 0, postgreSQLTime = 0, influxDBTime = 0;
        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        File outFile = Paths.get(outFolder, "timeInitialization.csv").toFile();
        boolean addHeader = outFile.length() == 0;
        CsvWriter csvWriter = new CsvWriter(new FileWriter(outFile, true), csvWriterSettings);

        long memorySize = 0;
//        SizeOf sizeOf = SizeOf.newInstance();
//
        Stopwatch stopwatch = Stopwatch.createUnstarted();
//        AbstractDataset dataset = createDataset();
//        List<String> measureNames = dataset.getMeasures().stream().map(m -> dataset.getHeader()[m]).collect(Collectors.toList());
//        String timeColName = dataset.getHeader()[dataset.getTimeCol()];
//
//        TTI tti = new TTI(dataset);
//        Query q0 = new Query(startTime, endTime, dataset.getMeasures(),
//                filters, new ViewPort(800, 300), groupyBy);
//
//        stopwatch.start();
//        tti.initialize(q0);
        ttiTime = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);

        if(postgreSQLCfg != null) {
            stopwatch.reset();
            PostgreSQLConnection postgreSQL = new PostgreSQLConnection(postgreSQLCfg);
            SQLQueryExecutor sqlQueryExecutor = postgreSQL.getSqlQueryExecutor(schema, table);
            sqlQueryExecutor.drop();
            stopwatch.start();
            sqlQueryExecutor.initialize(path);
            postgreSQLTime = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
        }

        if(influxDBCfg != null) {
            stopwatch.reset();
            InfluxDBConnection influxDBConnection = new InfluxDBConnection(influxDBCfg);
            InfluxDBQueryExecutor influxDBQueryExecutor = influxDBConnection.getSqlQueryExecutor(schema, table);
            influxDBQueryExecutor.drop();
            stopwatch.start();
            influxDBQueryExecutor.initialize(path);
            influxDBTime = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
        }

        stopwatch.reset();

//        memorySize = tti.calculateDeepMemorySize();

        if (addHeader) {
            csvWriter.writeHeaders("dataset", "mode", "TTI Time (sec)", "PostgreSQL Time (sec)",  "InfluxDB Time (sec)",  "Memory (Gb)");
        }

        csvWriter.addValue(table);
        csvWriter.addValue(command);
        csvWriter.addValue(ttiTime);
        csvWriter.addValue(postgreSQLTime);
        csvWriter.addValue(influxDBTime);
        csvWriter.addValue(memorySize);
        csvWriter.writeValuesToRow();
        csvWriter.close();
    }

    private void plotQuery() throws IOException, SQLException {
        String plotFolder = "plotQuery";
        recreateDir(Paths.get(outFolder, plotFolder).toString());
        String rawTTiResultsPath = Paths.get(outFolder,"plotQuery", "rawResults").toString();
        String ttiResultsPath = Paths.get(outFolder, "plotQuery", "ttiResults").toString();
        String sqlResultsPath = Paths.get(outFolder, "plotQuery", "sqlResults").toString();
        String influxDBResultsPath = Paths.get(outFolder, "plotQuery", "influxDBResults").toString();
        ViewPort viewPort = new ViewPort(800, 300);
        AbstractDataset dataset = createDataset();
        Query ttiQuery = new Query(startTime, endTime, measures,
                filters, viewPort, groupyBy);
        List<String> measureNames = ttiQuery.getMeasures().stream().map(m -> dataset.getHeader()[m]).collect(Collectors.toList());

        TTI tti = new TTI(dataset);
        RawTTI rawTTI = new RawTTI(dataset);

        PostgreSQLConnection postgreSQL = new PostgreSQLConnection(postgreSQLCfg);
        InfluxDBConnection influxDBConnection = new InfluxDBConnection(influxDBCfg);

        InfluxDBQueryExecutor influxDBQueryExecutor = influxDBConnection.getSqlQueryExecutor(schema, table);
        SQLQueryExecutor sqlQueryExecutor = postgreSQL.getSqlQueryExecutor(schema, table);
        SQLQuery sqlQuery = new SQLQuery(startTime, endTime, measures, filters, viewPort, null);
        InfluxDBQuery influxDBQuery = new InfluxDBQuery(startTime, endTime, measureNames, filters, viewPort, null);

        TimeSeriesPlot timeSeriesPlot = new TimeSeriesPlot(Paths.get(outFolder, plotFolder).toString());

        QueryResults rawTtiQueryResults = rawTTI.executeQuery(ttiQuery);
        rawTtiQueryResults.toMultipleCsv(rawTTiResultsPath);
        timeSeriesPlot.build(rawTTiResultsPath);

        QueryResults ttiQueryResults = tti.executeQuery(ttiQuery);
        ttiQueryResults.toMultipleCsv(ttiResultsPath);
        timeSeriesPlot.build(ttiResultsPath);

        if(type.equals("postgres")) {
            QueryResults sqlQueryResults = sqlQueryExecutor.executeM4Query(sqlQuery);
            sqlQueryResults.toMultipleCsv(sqlResultsPath);
            timeSeriesPlot.build(sqlResultsPath);
        }
        if(type.equals("influx")) {
            QueryResults influxDBQueryResults = influxDBQueryExecutor.executeM4Query(influxDBQuery);
            influxDBQueryResults.toCsv(influxDBResultsPath);
            timeSeriesPlot.build(influxDBResultsPath);
        }
    }

    private void timeQueries() throws IOException, SQLException {
        File outFile = Paths.get(outFolder, "timeQueries", type, table, "results.csv").toFile();
        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        CsvWriter csvWriter = new CsvWriter(new FileWriter(outFile, false), csvWriterSettings);

        String rawTTiResultsPath = Paths.get(outFolder,"timeQueries", type, table, "rawResults").toString();
        String ttiResultsPath = Paths.get(outFolder,"timeQueries", type, table, "ttiResults").toString();
        String sqlResultsPath = Paths.get(outFolder,"timeQueries", type, table, "sqlResults").toString();
        String influxDBResultsPath = Paths.get(outFolder,"timeQueries", type, table, "influxDBResults").toString();

        Stopwatch stopwatch = Stopwatch.createUnstarted();
        stopwatch.start();
        AbstractDataset dataset = createDataset();
        TTI tti = new TTI(dataset);
        RawTTI rawTTI = new RawTTI(dataset);
        QueryMethod queryMethod = QueryMethod.M4_MULTI;
        PostgreSQLConnection postgreSQLConnection = new PostgreSQLConnection(postgreSQLCfg);
        InfluxDBConnection influxDBConnection = new InfluxDBConnection(influxDBCfg);
        InfluxDBQueryExecutor influxDBQueryExecutor = influxDBConnection.getSqlQueryExecutor(schema, table);
        SQLQueryExecutor sqlQueryExecutor = postgreSQLConnection.getSqlQueryExecutor(schema, table);

        Query q0 = new Query(startTime, endTime, queryMethod, measures,
                filters, new ViewPort(1000, 600), groupyBy);
        List<AbstractQuery> sequence = generateQuerySequence(q0, dataset);

        csvWriter.writeHeaders("dataset","query #", "operation", "timeRange", "TTI results size", "RAW TTI results size",
                "PostgreSQL results size", "InfluxDB results size", "TTI IO Count", "RAW TTI IO Count",
                "TTI Time (sec)", "RAW TTI Time (sec)", "PostgreSQL Time (sec)",  "InfluxDB Time (sec)",  "TTI Memory (b)", "Raw TTI Memory (b)");
        for (int i = 0; i < sequence.size(); i += 3) {
            QueryResults sqlQueryResults, influxDBQueryResults;
            double ttiTime = 0, rawTtiTIme = 0, postgreSQLTime = 0, influxDBTime = 0;
            Query query = (Query) sequence.get(i);
            SQLQuery sqlQuery = (SQLQuery) sequence.get(i + 1);
            InfluxDBQuery influxDBQuery = (InfluxDBQuery) sequence.get(i + 2);
            LOG.debug("Executing query " + i);

            stopwatch = Stopwatch.createStarted();
            QueryResults ttiQueryResults = tti.executeQuery(query);
            ttiTime = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);

            stopwatch.reset();
            stopwatch.start();
            QueryResults rawQueryResults = rawTTI.executeQuery(query);
            rawTtiTIme = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);

            int sqlQueryResultsSize = 0, influxDBQueryResultsSize = 0;
            if(type.equals("postgres")) {
                stopwatch.reset();
                stopwatch.start();
                sqlQueryResults = sqlQueryExecutor.execute(sqlQuery, QueryMethod.M4);
                if(sqlQueryResults.getData().size() != 0)
                    sqlQueryResultsSize = sqlQueryResults.getData().get(this.measures.get(0)).size();
                postgreSQLTime = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
                sqlQueryResults.toMultipleCsv(Paths.get(sqlResultsPath, "query_" + i).toString());
            }
            else if(type.equals("influx")) {
                stopwatch.reset();
                stopwatch.start();
                influxDBQueryResults = influxDBQueryExecutor.execute(influxDBQuery, QueryMethod.M4);
                if(influxDBQueryResults.getData().size() != 0)
                    influxDBQueryResultsSize = influxDBQueryResults.getData().get(0).size();
                influxDBTime = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
                influxDBQueryResults.toMultipleCsv(Paths.get(influxDBResultsPath, "query_" + i).toString());
            }
            long memorySize = tti.calculateDeepMemorySize();
            long rawMemorySize = rawTTI.calculateDeepMemorySize();
            ttiQueryResults.toMultipleCsv(Paths.get(ttiResultsPath, "query_" + i).toString());
            // rawQueryResults.toMultipleCsv(Paths.get(rawTTiResultsPath, "query_" + i).toString());

            csvWriter.addValue(table);
            csvWriter.addValue(i);
            csvWriter.addValue(query.getOpType());
            csvWriter.addValue(query.getFromDate() + " - " + query.getToDate());
            csvWriter.addValue(ttiQueryResults.getData().get(this.measures.get(0)).size());
            csvWriter.addValue(rawQueryResults.getData().get(this.measures.get(0)).size());
            csvWriter.addValue(sqlQueryResultsSize);
            csvWriter.addValue(influxDBQueryResultsSize);
            csvWriter.addValue(ttiQueryResults.getIoCount());
            csvWriter.addValue(rawQueryResults.getIoCount());
            csvWriter.addValue(ttiTime);
            csvWriter.addValue(rawTtiTIme);
            csvWriter.addValue(postgreSQLTime);
            csvWriter.addValue(influxDBTime);
            csvWriter.addValue(memorySize);
            csvWriter.addValue(rawMemorySize);
            csvWriter.writeValuesToRow();
        }
        csvWriter.close();
    }


    private List<AbstractQuery> generateQuerySequence(Query q0, AbstractDataset dataset) {
        Preconditions.checkNotNull(seqCount, "No sequence count specified.");
        Preconditions.checkNotNull(minShift, "Min query shift must be specified.");
        Preconditions.checkNotNull(maxShift, "Max query shift must be specified.");
        Preconditions.checkNotNull(minFilters, "Min filters must be specified.");
        Preconditions.checkNotNull(maxFilters, "Max filters must be specified.");

        QuerySequenceGenerator sequenceGenerator = new QuerySequenceGenerator(minShift, maxShift, minFilters, maxFilters, zoomFactor, dataset);
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
                PostgreSQLDataset postgreSQLDataset = new PostgreSQLDataset(postgreSQLCfg, schema, table, timeFormat);
                SerializationUtilities.storeSerializedObject(postgreSQLDataset, p);
                return postgreSQLDataset;
            case "influx":
                p = String.valueOf(Paths.get(outFolder, "metadata", "influx-" + table));
                if(new File(p).exists()) return (InfluxDBDataset) SerializationUtilities.loadSerializedObject(p);
                InfluxDBDataset influxDBDataset = new InfluxDBDataset(influxDBCfg, schema, table, timeFormat);
                SerializationUtilities.storeSerializedObject(influxDBDataset, p);
                return influxDBDataset;
            default:
                break;
        }
        return null;
    }

}
