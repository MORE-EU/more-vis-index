package eu.more2020.visual.util;

import eu.more2020.visual.domain.AggregateInterval;
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
        ZonedDateTime endDateTime = DateTimeUtil.getIntervalStart(endTime - 1, interval, unit, zoneId);
        return (int) (unit.between(startDateTime, endDateTime) / interval) + 1;
    }

    /**
     * Returns the optimal M4 sampling interval in a specific range.
     * @param from The start timestamp to find the M4 sampling interval for (in timestamps)
     * @param to The end timestamp to find the M4 sampling interval for (in timestamps)
     * @param viewPort  A viewport object that contains information about the chart that the user is visualizing
     * @return A Duration
     */
    public static Duration optimalM4(long from, long to, ViewPort viewPort) {
        int noOfGroups = viewPort.getWidth();
        long millisInRange = Duration.of(to - from, ChronoUnit.MILLIS).toMillis() / noOfGroups;
        return Duration.of(millisInRange, ChronoUnit.MILLIS);
    }

    /**
     * Returns a sampling interval less than or equal to the given interval, so that it can divide exactly longer durations,
     * based on the gregorian calendar.
     * For this, we check how the given interval divides the next
     * calendar based frequency (e.g seconds -> minute, minute -> hour etc.) To get the closest exact division,
     * we floor the remainder (if it is decimal) and add 1 to it. Then, we divide the remaining number with how many
     * times the current frequency fits onto the next, to get the calendar based sampling interval that is closest to the given one.
     *
     * @param samplingInterval The sampling interval
     * @return A Duration
     */
    private static Duration maxCalendarInterval(Duration samplingInterval) {
        // Get each part that makes up a calendar date. The first non-zero is its "granularity".
        int days = (int) samplingInterval.toDaysPart();
        int hours = samplingInterval.toHoursPart();
        int minutes = samplingInterval.toMinutesPart();
        int seconds = samplingInterval.toSecondsPart();
        int millis = samplingInterval.toMillisPart();
        long t = 0L;
        if(days != 0){
            t = Duration.ofDays(30).toMillis();
        }
        else if(hours != 0){
            t = Duration.ofHours(24).toMillis();;
        }
        else if(minutes != 0){
            t = Duration.ofMinutes(60).toMillis();;
        }
        else if(seconds != 0){
            t = Duration.ofSeconds(60).toMillis();;
        }
        else if(millis != 0){
            t = Duration.ofSeconds(1000).toMillis();;
        }
        double divisor = t * 1.0 / samplingInterval.toMillis();
        double flooredDivisor = Math.floor(divisor);
        divisor = flooredDivisor == divisor ? divisor : flooredDivisor + 1;
        return Duration.of(t, ChronoUnit.MILLIS).dividedBy((long) divisor);
    }

    public static Duration accurateCalendarInterval(long from, long to, ViewPort viewPort, float accuracy) {
        Duration optimalM4Interval = optimalM4(from, to, viewPort);
        Duration calendarBasedInterval = maxCalendarInterval(optimalM4Interval);
        Duration timeRangeDuration  = Duration.of(to - from, ChronoUnit.MILLIS);
        long divisor = 0;
        float percent = 1;
        Duration G4samplingInterval = null;
        while(percent > (1.0 - accuracy)){
            G4samplingInterval = calendarBasedInterval.dividedBy((long) Math.pow(2, divisor ++));
            int g4Width = (int) timeRangeDuration.dividedBy(G4samplingInterval);
            percent = (float) viewPort.getWidth() / g4Width;
        }
        return G4samplingInterval;
    }

    public static AggregateInterval aggregateCalendarInterval(Duration interval){

        long aggregateInterval = interval.toMillis();
        ChronoUnit aggregateChronoUnit = ChronoUnit.MILLIS;
        if(aggregateInterval % 1000 == 0){
            aggregateInterval = aggregateInterval / 1000;
            aggregateChronoUnit = ChronoUnit.SECONDS;
            if(aggregateInterval % 60 == 0){
                aggregateInterval = aggregateInterval / 60;
                aggregateChronoUnit = ChronoUnit.MINUTES;
                if(aggregateInterval % 60 == 0){
                    aggregateInterval = aggregateInterval / 60;
                    aggregateChronoUnit = ChronoUnit.HOURS;
                    if(aggregateInterval % 24 == 0){
                        aggregateInterval = aggregateInterval / 24;
                        aggregateChronoUnit = ChronoUnit.DAYS;
                    }
                }
            }
        }
        return new AggregateInterval(aggregateInterval, aggregateChronoUnit);
    }

    public static void M4_percent(TimeRange timeRange, ViewPort viewPort) {
        int noOfGroups = viewPort.getWidth();
        long millisInRange = Duration.of(timeRange.getTo() - timeRange.getFrom(), ChronoUnit.MILLIS).toMillis() / noOfGroups;
        Duration M4samplingInterval = Duration.of(millisInRange, ChronoUnit.MILLIS);

        // Get each part that makes up a calendar date. The first non-zero is its "granularity".
        int days = (int) M4samplingInterval.toDaysPart();
        int hours = M4samplingInterval.toHoursPart();
        int minutes = M4samplingInterval.toMinutesPart();
        int seconds = M4samplingInterval.toSecondsPart();
        int millis = M4samplingInterval.toMillisPart();
        long t = 0L;
        if(days != 0){
            t = Duration.ofDays(30).toMillis();
        }
        else if(hours != 0){
            t = Duration.ofHours(24).toMillis();;
        }
        else if(minutes != 0){
            t = Duration.ofMinutes(60).toMillis();;
        }
        else if(seconds != 0){
            t = Duration.ofSeconds(60).toMillis();;
        }
        else if(millis != 0){
            t = Duration.ofSeconds(1000).toMillis();;
        }
        double divisor = t * 1.0 / M4samplingInterval.toMillis();
        double flooredDivisor = Math.floor(divisor);
        divisor = flooredDivisor == divisor ? divisor : flooredDivisor + 1;
        Duration timeRangeDuration  = Duration.of(timeRange.getTo() - timeRange.getFrom(), ChronoUnit.MILLIS);
        System.out.println("M4: " + M4samplingInterval);
        System.out.println(viewPort.getWidth());
        float percent = 1;
        int i = 1;
        Duration G4samplingInterval = null;
        while(percent > 0.1){
            G4samplingInterval = Duration.of(t, ChronoUnit.MILLIS).dividedBy((long) divisor * (2L * i ++ ));
            System.out.println("G4: " + G4samplingInterval);
            int g4Width = (int) timeRangeDuration.dividedBy(G4samplingInterval);
            percent = (float) viewPort.getWidth() / g4Width;
            System.out.println(percent);
        }

        System.out.println(G4samplingInterval);
    }
}
