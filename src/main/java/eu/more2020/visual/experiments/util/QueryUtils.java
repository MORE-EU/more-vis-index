package eu.more2020.visual.experiments.util;


import eu.more2020.visual.util.DateTimeUtil;

import java.text.ParseException;
import java.time.format.DateTimeFormatter;


public class QueryUtils {

    public static Long convertToEpoch(String s) throws ParseException {
        return DateTimeUtil.parseDateTimeString(s);
    }

}