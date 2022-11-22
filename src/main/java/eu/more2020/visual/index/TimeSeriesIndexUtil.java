package eu.more2020.visual.index;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import java.time.LocalDateTime;
import java.time.temporal.TemporalField;
import java.util.Arrays;
import java.util.List;

import static java.time.temporal.ChronoField.*;

public class TimeSeriesIndexUtil {
    public static final List<TemporalField> TEMPORAL_HIERARCHY = Arrays.asList(YEAR, MONTH_OF_YEAR, DAY_OF_MONTH, HOUR_OF_DAY, MINUTE_OF_HOUR, SECOND_OF_MINUTE, MILLI_OF_SECOND);
    private static BiMap<String, TemporalField> temporalFieldMap = HashBiMap.create();

    static {
        temporalFieldMap.put("YEAR", YEAR);
        temporalFieldMap.put("MONTH", MONTH_OF_YEAR);
        temporalFieldMap.put("DAY", DAY_OF_MONTH);
        temporalFieldMap.put("HOUR", HOUR_OF_DAY);
        temporalFieldMap.put("MINUTE", MINUTE_OF_HOUR);
        temporalFieldMap.put("SECOND", SECOND_OF_MINUTE);
        temporalFieldMap.put("MILLI", MILLI_OF_SECOND);
    }

    public static TemporalField getTemporalFieldByName(String name) {
        return temporalFieldMap.get(name);
    }

    public static String getTemporalFieldName(TemporalField temporalField) {
        return temporalFieldMap.inverse().get(temporalField);
    }

    public static int getTemporalLevelIndex(String temporalLevel) {
        return TEMPORAL_HIERARCHY.indexOf(getTemporalFieldByName(temporalLevel));
    }

    public static LocalDateTime truncate(LocalDateTime dateTime, TemporalField temporalField) {
        return dateTime.truncatedTo(temporalField.getBaseUnit());
    }

    public static LocalDateTime truncate(LocalDateTime dateTime, String unit) {
        return dateTime.truncatedTo(getTemporalFieldByName(unit).getBaseUnit());
    }



}

