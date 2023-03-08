package eu.more2020.visual.domain;

import java.time.ZonedDateTime;

public interface PixelAggregatedDataPoint extends AggregatedDataPoint  {

    public ZonedDateTime getSubPixel();
    public  ZonedDateTime getPixel();
    public  ZonedDateTime getNextPixel();
    public  AggregateInterval getInterval();
    public PixelAggregatedDataPoint persist();
    public boolean overlaps();
}
