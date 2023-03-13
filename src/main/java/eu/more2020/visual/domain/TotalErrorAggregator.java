package eu.more2020.visual.domain;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class TotalErrorAggregator implements Consumer<PixelAggregatedDataPoint>{

    private final List<PixelColumnErrorAggregator> error;
    private final StatsAggregator stats;
    private final List<Integer> measures;
    private final ViewPort viewport;
    private final AggregateInterval interval;

    private int currentPixelColumn;
    private boolean finishedIt = false;

    public TotalErrorAggregator(StatsAggregator stats, List<Integer> measures, ViewPort viewport, AggregateInterval interval) {
        this.error = new ArrayList<>();
        this.stats = stats;
        this.measures = measures;
        this.viewport = viewport;
        this.interval = interval;
    }

    public void accept(PixelAggregatedDataPoint pixelAggregatedDataPoint) {
        error.get(currentPixelColumn).accept(pixelAggregatedDataPoint);
        if(pixelAggregatedDataPoint.isOverlapping()) error.get(currentPixelColumn + 1).accept(pixelAggregatedDataPoint);
    }

    public void add(ZonedDateTime pixel){
        this.error.add(new PixelColumnErrorAggregator(measures, stats, interval, pixel, viewport.getHeight()));
        currentPixelColumn++;
    }

    public void initialize(ZonedDateTime startPixel){
        ZonedDateTime nextPixel = startPixel.plus(interval.getInterval(), interval.getChronoUnit());
        add(startPixel);
        add(nextPixel);
        currentPixelColumn = 0;
    }

    public void update(){
        if(finishedIt) return;
        update(currentPixelColumn, currentPixelColumn - 1);
        if(currentPixelColumn == viewport.getWidth() - 1) finishedIt = true;
    }

    public void update(int update){
        update(currentPixelColumn, update);
    }

    public void update(int current, int update){
        if (currentPixelColumn != 0) // update prev error for intra col
            error.get(update).accept(error.get(current));
    }

    public class PixelColumnErrorAggregator implements Consumer<PixelAggregatedDataPoint>, PixelColumnError, Serializable {

        private int[] minId;
        private int[] maxId;

        private double[] innerColsError;
        private double[] intraColsError;

        private final Stats stats;
        private final List<Integer> measures;
        private final AggregateInterval interval;
        private final ZonedDateTime pixelColumn;
        private final int height;
        private int count = 0;

        public PixelColumnErrorAggregator(List<Integer> measures, Stats stats, AggregateInterval interval, ZonedDateTime pixelColumn, int height) {
            this.measures = measures;
            this.height = height;
            this.stats = stats;
            this.interval = interval;
            this.pixelColumn = pixelColumn;
            int length = measures.size();
            innerColsError = new double[length];
            intraColsError = new double[length];
            minId = new int[length];
            maxId = new int[length];
        }

        public void clear() {
            Arrays.fill(minId, 0);
            Arrays.fill(maxId, 0);
            Arrays.fill(innerColsError,  0d);
            Arrays.fill(intraColsError, 0d);
        }

        @Override
        public int getId() {
            return 0;
        }

        @Override
        public double getInnerColError(int measure) {
            if (count == 0) {
                throw new IllegalStateException("No pixel columns added to this error aggregator yet.");
            }
            return innerColsError[getMeasureIndex(measure)];
        }

        @Override
        public double intraColError(int measure) {
            if (count == 0) {
                throw new IllegalStateException("No pixel columns added to this error aggregator yet.");
            }
            return intraColsError[getMeasureIndex(measure)];
        }

        @Override
        public void accept(PixelAggregatedDataPoint pixelAggregatedDataPoint) {
            ++count;
            int i = 0;
            for (int m : measures) {
                int min = getPixelId(m, pixelAggregatedDataPoint.getStats().getMinValue(m));
                int max = getPixelId(m, pixelAggregatedDataPoint.getStats().getMinValue(m));
                long minTimestamp = pixelAggregatedDataPoint.getStats().getMinTimestamp(m);
                long maxTimestamp = pixelAggregatedDataPoint.getStats().getMaxTimestamp(m);
                // if its fully overlapping initialize
                if(!pixelAggregatedDataPoint.isOverlapping()){
                    minId[i] = min;
                    maxId[i] = max;
                } else { // check if min max falls in pixel column
                    boolean containsMin = minTimestamp <= (pixelColumn.plus(interval.getInterval(), interval.getChronoUnit()).toInstant().toEpochMilli()) &&
                            minTimestamp >= (pixelColumn.toInstant().toEpochMilli()) ;
                    boolean containsMax = maxTimestamp <= (pixelColumn.plus(interval.getInterval(), interval.getChronoUnit()).toInstant().toEpochMilli()) &&
                            maxTimestamp >= (pixelColumn.toInstant().toEpochMilli());
                    minId[i] = containsMin ? min : minId[i];
                    maxId[i] = containsMax ? max : maxId[i];
                }
                i++;
            }

        }

        public void accept(PixelColumnErrorAggregator pixelColumnErrorAggregator) {

        }

        private int getMeasureIndex(int measure) {
            return measures.indexOf(measure);
        }

        public int getPixelId(int m, double value){
            double range = Math.abs(stats.getMaxValue(m)) + Math.abs(stats.getMinValue(m));
            double bin_size = range / height;
            int v = (int) Math.floor(value / bin_size);
            return v;
        }

    }
}

