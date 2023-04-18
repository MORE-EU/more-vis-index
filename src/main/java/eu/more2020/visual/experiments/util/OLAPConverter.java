package eu.more2020.visual.experiments.util;

import com.beust.jcommander.IStringConverter;

import java.time.temporal.ChronoField;

public class OLAPConverter implements IStringConverter<ChronoField> {
    @Override
    public ChronoField convert(String value) {
        switch (value) {
            case "hod":
                return ChronoField.HOUR_OF_DAY;
            case "dow":
                return ChronoField.DAY_OF_WEEK;
            case "dom":
                return ChronoField.DAY_OF_MONTH;
            default:
                throw new UnsupportedOperationException();
        }
    }
}
