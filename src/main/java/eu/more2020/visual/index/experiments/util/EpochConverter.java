package eu.more2020.visual.index.experiments.util;

import com.beust.jcommander.IStringConverter;

import java.text.ParseException;


public class EpochConverter implements IStringConverter<Long>  {

    @Override
    public Long convert(String s) {
        try {
            return QueryUtils.convertToEpoch(s);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }
}
