package eu.more2020.visual.util;

import eu.more2020.visual.domain.TimeRange;
import eu.more2020.visual.domain.ViewPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public class DateTimeUtil {

    public static final ZoneId UTC = ZoneId.of("UTC");
    public final static String DEFAULT_FORMAT = "yyyy-MM-dd[ HH:mm:ss]";
    public final static DateTimeFormatter DEFAULT_FORMATTER = DateTimeFormatter.ofPattern(DEFAULT_FORMAT);
    private static final Logger LOG = LoggerFactory.getLogger(DateTimeUtil.class);

    public static long parseDateTimeString(String s, DateTimeFormatter formatter, ZoneId zoneId) {
        return LocalDateTime.parse(s, formatter).atZone(zoneId).toInstant().toEpochMilli();
    }

    public static String format(final long timeStamp) {
        return formatTimeStamp(DEFAULT_FORMATTER, timeStamp);
    }

    public static String format(final long timeStamp, final ZoneId zone) {
        return format(DEFAULT_FORMATTER, timeStamp, zone);
    }


    public static String format(final String format, final long timeStamp, final ZoneId zone) {
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
        return Instant.ofEpochMilli(timeStamp)
                .atZone(zone)
                .format(formatter);
    }

    public static String format(final DateTimeFormatter formatter, final long timeStamp, final ZoneId zone) {
        return Instant.ofEpochMilli(timeStamp)
                .atZone(zone)
                .format(formatter);
    }

    public static String formatTimeStamp(final long timeStamp) {
        return formatTimeStamp(DEFAULT_FORMATTER, timeStamp);
    }

    public static String formatTimeStamp(final String format, final long timeStamp) {
        return format(format, timeStamp, UTC);
    }

    public static String formatTimeStamp(final DateTimeFormatter formatter, final long timeStamp) {
        return format(formatter, timeStamp, UTC);
    }


    /**
     * Returns the start date time of the interval that the timestamp belongs,
     * based on the given interval, unit and timezone.
     * It follows a calendar-based approach, considering intervals that align with the start of day,
     * month, etc.
     *
     * @param timestamp The timestamp to find an interval for, in milliseconds from epoch
     * @param interval  The interval, which together with the unit, fully define the interval
     * @param unit      The unit of the interval
     * @param zoneId    The zone id.
     * @return A ZonedDateTime instance set to the start of the interval this timestamp belongs
     * @throws IllegalArgumentException if the timestamp is negative or the interval is less than one
     */
    public static ZonedDateTime getIntervalStart(long timestamp, int interval,
                                                 ChronoUnit unit, ZoneId zoneId) {
        if (timestamp < 0) {
            throw new IllegalArgumentException("Timestamp cannot be less than zero");
        }
        if (interval < 1) {
            throw new IllegalArgumentException("Interval interval must be greater than zero");
        }

        if (zoneId == null) {
            zoneId = ZoneId.of("UTC");
        }


        ZonedDateTime dateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), zoneId);


        switch (unit) {
            case MILLIS:
                if (1000 % interval == 0) {
                    dateTime = dateTime.withNano(0);
                } else {
                    dateTime = dateTime.withNano(0)
                            .withSecond(0);
                }
                break;
            case SECONDS:
                if (60 % interval == 0) {
                    dateTime = dateTime.withNano(0)
                            .withSecond(0);
                } else {
                    dateTime = dateTime.withNano(0)
                            .withSecond(0).withMinute(0);
                }
                break;
            case MINUTES:
                if (60 % interval == 0) {
                    dateTime = dateTime.withNano(0)
                            .withSecond(0).withMinute(0);
                } else {
                    dateTime = dateTime.withNano(0)
                            .withSecond(0).withMinute(0).withHour(0);
                }
                break;
            case HOURS:
                if (24 % interval == 0) {
                    dateTime = dateTime.withNano(0)
                            .withSecond(0).withMinute(0).withHour(0);
                } else {
                    dateTime = dateTime.withNano(0)
                            .withSecond(0).withMinute(0).withHour(0).withDayOfMonth(0);
                }
                break;
            case DAYS:
                if (interval == 1) {
                    dateTime = dateTime.withNano(0)
                            .withSecond(0).withMinute(0).withHour(0).withDayOfMonth(0);
                } else {
                    dateTime = dateTime.withNano(0)
                            .withSecond(0).withMinute(0).withHour(0).withDayOfMonth(0).withMonth(0);
                }
                break;
            case MONTHS:
            case YEARS:
                dateTime = dateTime.withNano(0)
                        .withSecond(0).withMinute(0).withHour(0).withDayOfMonth(0).withMonth(0);
                break;
            default:
                throw new IllegalArgumentException("Unexpected unit of type: "
                        + unit);
        }

        if (dateTime.toInstant().toEpochMilli() == timestamp) {
            return dateTime;
        }
        while (dateTime.toInstant().toEpochMilli() <= timestamp) {
            dateTime = dateTime.plus(interval, unit);
        }
        dateTime = dateTime.minus(interval, unit);
        return dateTime;
    }


    public static int numberOfIntervals(final long startTime, final long endTime, int interval,
                                        ChronoUnit unit, ZoneId zoneId) {
        ZonedDateTime startDateTime = DateTimeUtil.getIntervalStart(startTime, interval, unit, zoneId);
        ZonedDateTime endDateTime = DateTimeUtil.getIntervalStart(endTime, interval, unit, zoneId);
        return (int) (unit.between(startDateTime, endDateTime) / interval) + 1;
    }


    public static void M4(TimeRange timeRange, Duration samplingInterval, ViewPort viewPort) {
//        int noOfGroups = Math.floorDiv(viewPort.getWidth(), 4);
        int noOfGroups = viewPort.getWidth();
        long pointsInRange = Duration.of(timeRange.getFrom() - timeRange.getTo(), ChronoUnit.MILLIS)
                .dividedBy(samplingInterval);
        int groupSize = (int) pointsInRange / noOfGroups;
        System.out.println(samplingInterval.multipliedBy(groupSize));
    }


}
