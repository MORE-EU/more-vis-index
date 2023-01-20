package eu.more2020.visual.experiments.util.InfluxDB.InitQueries;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import com.opencsv.bean.CsvBindByName;

@Measurement(name = "bebeze")
public class BEBEZE {
    @Column(tag = true)
    @CsvBindByName(column = "datetime")
    private String datetime;

    @Column
    @CsvBindByName(column = "active_power")
    private String active_power;

    @Column
    @CsvBindByName(column = "roto_speed")
    private String roto_speed;

    @Column
    @CsvBindByName(column = "wind_speed")
    private String wind_speed;

    @Column
    @CsvBindByName(column = "close")
    private String close;

    @Column(timestamp = true)
    @CsvBindByName(column = "timestamp")
    private String timestamp;
}