package eu.more2020.visual.middleware.domain.InfluxDB.InitQueries;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import com.opencsv.bean.AbstractBeanField;
import com.opencsv.bean.CsvBindByName;
import com.opencsv.bean.CsvCustomBindByName;
import eu.more2020.visual.middleware.experiments.util.QueryUtils;

import java.text.ParseException;
import java.time.Instant;

@Measurement(name = "synthetic128m")
public class SYNTHETIC128M {

    @Column(timestamp = true)
    @CsvCustomBindByName(column="date", converter = EpochConverter.class)
    private Instant date;

    @Column
    @CsvBindByName(column = "value_1")
    private Double value_1;

    @Column
    @CsvBindByName(column = "value_2")
    private Double value_2;

    @Column
    @CsvBindByName(column = "value_3")
    private Double value_3;

    @Column
    @CsvBindByName(column = "value_4")
    private Double value_4;

    @Column
    @CsvBindByName(column = "value_5")
    private Double value_5;

    public static class EpochConverter extends AbstractBeanField {

        @Override
        public Instant convert(String s) {
            try {
                return Instant.ofEpochMilli(QueryUtils.convertToEpoch(s, "yyyy-MM-dd[ HH:mm:ss]"));
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

}