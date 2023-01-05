package eu.more2020.visual.experiments.util;


import com.google.common.collect.Range;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class QueryUtils {


    public static Long convertToEpoch(String s) throws ParseException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd[HH:mm:ss]");
        Date date = df.parse(s);
        return date.getTime();
    }

}