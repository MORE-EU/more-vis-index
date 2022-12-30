package eu.more2020.visual.experiments.util;

import com.beust.jcommander.IStringConverter;
import gr.athenarc.imsi.visualfacts.Rectangle;


public class RectangleConverter implements IStringConverter<Rectangle> {

    @Override
    public Rectangle convert(String s) {
        return QueryUtils.convertToRectangle(s);
    }
}
