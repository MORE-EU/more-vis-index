package eu.more2020.visual.middleware.experiments.util;


import eu.more2020.visual.middleware.util.DateTimeUtil;

import java.text.ParseException;


public class QueryUtils {

    public static Long convertToEpoch(String s) throws ParseException {
        return DateTimeUtil.parseDateTimeString(s);
    }

    public static Long convertToEpoch(String s, String timeFormat) throws ParseException {
        return DateTimeUtil.parseDateTimeString(s, timeFormat);
    }

}