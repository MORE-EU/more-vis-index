package eu.more2020.visual.experiments.util;

import com.beust.jcommander.IStringConverter;
import eu.more2020.visual.domain.ViewPort;

import java.time.temporal.ChronoField;

public class ViewPortConverter implements IStringConverter<ViewPort> {
    @Override
    public ViewPort convert(String value) {
        return new ViewPort(Integer.parseInt(value.split(",")[0]), Integer.parseInt(value.split(",")[1]));
    }
}
