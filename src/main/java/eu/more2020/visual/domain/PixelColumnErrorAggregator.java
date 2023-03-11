package eu.more2020.visual.domain;

import javax.swing.text.View;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class PixelColumnErrorAggregator implements Consumer<PixelAggregatedDataPoint>, PixelColumnError, Serializable {

    private int[] minId;
    private int[] maxId;

    private double[] innerColsError;
    private double[] intraColsError;

    private final Stats stats;
    private final int height;
    private final List<Integer> measures;
    private int count = 0;

    public PixelColumnErrorAggregator(List<Integer> measures, Stats stats, int height) {
        this.measures = measures;
        this.height = height;
        this.stats = stats;
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
        for (int m : measures) {
            int min = getPixelId(m, pixelAggregatedDataPoint.getStats().getMinValue(m));
            int max = getPixelId(m, pixelAggregatedDataPoint.getStats().getMinValue(m));
            long minTimestamp = pixelAggregatedDataPoint.getStats().getMinTimestamp(m);
            long maxTimestamp = pixelAggregatedDataPoint.getStats().getMinTimestamp(m);
            // if its fully overlapping initialize
            if(!pixelAggregatedDataPoint.isOverlapping()){
                minId[m] = min;
                maxId[m] = max;
            } else { // check if min max falls in pixel column

            }
        }

    }

    public void accept(PixelColumnErrorAggregator pixelColumnErrorAggregator) {
        ++count;
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
