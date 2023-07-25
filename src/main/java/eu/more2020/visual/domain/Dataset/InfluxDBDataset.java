package eu.more2020.visual.domain.Dataset;

import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import eu.more2020.visual.domain.InfluxDB.InfluxDBConnection;
import eu.more2020.visual.datasource.QueryExecutor.InfluxDBQueryExecutor;
import eu.more2020.visual.domain.InfluxDB.InitQueries.MANUFACTURING_EXP;
import eu.more2020.visual.domain.TimeRange;
import eu.more2020.visual.index.TTI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

public class InfluxDBDataset extends AbstractDataset {
    private static final Logger LOG = LoggerFactory.getLogger(TTI.class);

    private final String influxDBCfg;
    private final String bucket;
    private final String measurement;
    private final String timeFormat;


    public InfluxDBDataset(String influxDBCfg, String bucket, String measurement, String timeFormat) {
        this.influxDBCfg = influxDBCfg;
        this.bucket = bucket;
        this.measurement = measurement;
        this.timeFormat = timeFormat;
        this.fillInfluxDBDatasetInfo();
    }

    private void fillInfluxDBDatasetInfo() {
        List<FluxTable> fluxTables;
        InfluxDBConnection influxDBConnection = new InfluxDBConnection(influxDBCfg);
        String firstQuery = "from(bucket:\"" + bucket + "\")\n" +
                "  |> range(start: 1970-01-01T00:00:00.000Z, stop: 2150-01-01T00:00:00.999Z)\n" +
                "  |> filter(fn: (r) => r[\"_measurement\"] == \"" + measurement + "\")\n" +
                "  |> limit(n: 2)\n" +
                "  |> yield(name:\"first\")\n";
        InfluxDBQueryExecutor influxDBQueryExecutor = influxDBConnection.getSqlQueryExecutor(bucket, measurement);
        fluxTables = influxDBQueryExecutor.execute(firstQuery);

        Set<String> header = new LinkedHashSet<>();
        long from = Long.MAX_VALUE;
        long second = 0L;

        for(FluxTable fluxTable : fluxTables) {
            int i = 0;
            for (FluxRecord record : fluxTable.getRecords()) {
                if (i == 1) second = Objects.requireNonNull(record.getTime()).toEpochMilli();
                header.add(record.getField());
                long time = Objects.requireNonNull(record.getTime()).toEpochMilli();
                from = Math.min(from, time);
                i++;
            }
        }



        String lastQuery =  "from(bucket:\"" + bucket + "\")\n" +
                "  |> range(start: 0, stop:2120-01-01T00:00:00.000Z)\n" +
                "  |> filter(fn: (r) => r[\"_measurement\"] == \"" + measurement + "\")\n" +
                "  |> keep(columns: [\"_time\"])\n" +
                "  |> last(column: \"_time\")\n";

        fluxTables = influxDBQueryExecutor.execute(lastQuery);
        FluxRecord record = fluxTables.get(0).getRecords().get(0);
        long to = record.getTime().toEpochMilli();

        setSamplingInterval(Duration.of(second - from, ChronoUnit.MILLIS));
        setTimeRange(new TimeRange(from, to));
        setHeader(header.toArray(new String[0]));

    }

    @Override
    public String getTimeFormat() {
        return timeFormat;
    }

    @Override
    public List<Integer> getMeasures() {
        int[] measures = new int[getHeader().length];
        for(int i = 0; i < measures.length; i++)
            measures[i] = i;
        return Arrays.stream(measures)
                .boxed()
                .collect(Collectors.toList());
    }

    public String getConfig(){
        return influxDBCfg;
    }

    @Override
    public String getSchema(){
        return bucket;
    }

    @Override
    public String getName(){
        return measurement;
    }


}
