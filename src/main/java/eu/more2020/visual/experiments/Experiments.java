package eu.more2020.visual.experiments;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import eu.more2020.visual.domain.*;
import eu.more2020.visual.domain.Dataset.AbstractDataset;
import eu.more2020.visual.domain.Dataset.CsvDataset;
import eu.more2020.visual.domain.Dataset.ParquetDataset;
import eu.more2020.visual.domain.Query.*;
import eu.more2020.visual.experiments.util.EpochConverter;
import eu.more2020.visual.experiments.util.FilterConverter;
import eu.more2020.visual.experiments.util.InfluxDB.InfluxDB;
import eu.more2020.visual.experiments.util.QuerySequenceGenerator;
import eu.more2020.visual.experiments.util.SyntheticDatasetGenerator;
import eu.more2020.visual.index.TTI;
import eu.more2020.visual.experiments.util.QueryExecutor.InfluxDBQueryExecutor;
import eu.more2020.visual.experiments.util.PostgreSQL.PostgreSQL;
import eu.more2020.visual.experiments.util.QueryExecutor.SQLQueryExecutor;
import org.ehcache.sizeof.SizeOf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;


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
    public Integer timeCol;

    @Parameter(names = "-timeColName", description = "Datetime Column for Parquet files")
    public String timeColName;

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
    @Parameter(names = "-postgreSQLCfg", description = "PostgreSQL config file path")
    private String postgreSQLCfg;
    @Parameter(names = "-influxDBCfg", description = "InfluxDB config file path")
    private String influxDBCfg;
    @Parameter(names = "-table", description = "PostgreSQL/InfluxDB table name to query")
    private String table;
    @Parameter(names = "--measureMem", description = "Measure index memory after every query in the sequence")
    private boolean measureMem = false;


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

    private void timeInitialization() throws IOException, ClassNotFoundException, SQLException {
        Preconditions.checkNotNull(path, "You must define the input path.");
        Preconditions.checkNotNull(outFile, "No out file specified.");
        double ttiTime = 0, postgreSQLTime = 0, influxDBTime = 0;
        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        boolean addHeader = new File(outFile).length() == 0;

        CsvWriter csvWriter = new CsvWriter(new FileWriter(outFile, true), csvWriterSettings);

        long memorySize = 0;
        SizeOf sizeOf = SizeOf.newInstance();

        Stopwatch stopwatch = Stopwatch.createUnstarted();
        AbstractDataset dataset = createDataset();
        TTI tti = new TTI(dataset);
        Query q0 = new Query(startTime, endTime, dataset.getMeasures(), filters, new ViewPort(800, 300));
        stopwatch.start();
        tti.initialize(q0);
        ttiTime = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);

//        stopwatch.reset();
//        PostgreSQL postgreSQL = new PostgreSQL(postgreSQLCfg);
//        SQLQueryExecutor sqlQueryExecutor = postgreSQL.createQueryExecutor(path, table);
//        sqlQueryExecutor.drop();
//        stopwatch.start();
//        sqlQueryExecutor.initialize();
//        postgreSQLTime = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
//
//        stopwatch.reset();
//        InfluxDB influxDB = new InfluxDB(influxDBCfg);
//        InfluxDBQueryExecutor influxDBQueryExecutor = influxDB.createQueryExecutor(path, table);
//        influxDBQueryExecutor.drop();
//        stopwatch.start();
//        influxDBQueryExecutor.initialize();
//        influxDBTime = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);

        stopwatch.reset();
        try {
            memorySize = sizeOf.deepSizeOf(tti);
        } catch (Exception e) {
        }

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

    private void timeQueries() throws IOException, SQLException {
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
        PostgreSQL postgreSQL = new PostgreSQL(postgreSQLCfg);
        InfluxDB influxDB = new InfluxDB(influxDBCfg);
        SQLQueryExecutor sqlQueryExecutor = postgreSQL.createQueryExecutor(path, table);
        InfluxDBQueryExecutor influxDBQueryExecutor = influxDB.createQueryExecutor(path, table);
        Query q0 = new Query(startTime, endTime, dataset.getMeasures(), filters, new ViewPort(800, 300));
        tti.initialize(q0);


        List<AbstractQuery> sequence = generateQuerySequence(q0, dataset);

        if (addHeader) {
            csvWriter.writeHeaders("dataset", "mode", "query #", "timeRange", "results size", "IO Count",  "TTI Time (sec)", "PostgreSQL Time (sec)",  "InfluxDB Time (sec)",  "Memory (Gb)");
        }

        for (int i = 0; i < sequence.size(); i+=3) {
            double ttiTime = 0, postgreSQLTime = 0, influxDBTime = 0;
            Query query = (Query) sequence.get(i);
            SQLQuery sqlQuery = (SQLQuery) sequence.get(i + 1);
            InfluxQLQuery influxQLQuery = (InfluxQLQuery) sequence.get(i + 2);
            LOG.debug("Executing query " + i);

            stopwatch = Stopwatch.createStarted();
            QueryResults queryResults = tti.executeQuery(query);
            ttiTime = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);

//            stopwatch.reset();
//            stopwatch.start();
//            sqlQueryExecutor.execute(sqlQuery, QueryMethod.M4);
//            postgreSQLTime = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
//
//            stopwatch.reset();
//            stopwatch.start();
//            influxDBQueryExecutor.execute(influxQLQuery, QueryMethod.M4);
//            influxDBTime = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);

            try {
                memorySize = sizeOf.deepSizeOf(tti);
            } catch (Exception e) {
            }
            csvWriter.addValue(table);
            csvWriter.addValue(command);
            csvWriter.addValue(i);
            csvWriter.addValue(query.getFromDate() + " - " + query.getToDate());
            csvWriter.addValue(queryResults.getData().get(this.measures.get(0)).size());
            csvWriter.addValue(queryResults.getIoCount());
            csvWriter.addValue(ttiTime);
            csvWriter.addValue(postgreSQLTime);
            csvWriter.addValue(influxDBTime);
            csvWriter.addValue(memorySize);
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

    private AbstractDataset createDataset() throws IOException {
        switch (type.toLowerCase(Locale.ROOT)) {
            case "csv":
                return new CsvDataset(path, "0", "test", timeCol, measures, hasHeader, timeFormat, delimiter);
            case "parquet":
                return new ParquetDataset(path, "0", "test", timeColName, measureNames, timeFormat);
            default:
                break;
        }
        return null;
    }

}
