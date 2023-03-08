package eu.more2020.visual.domain;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class PixelColumnErrorAggregator implements Consumer<PixelAggregatedDataPoint>, PixelColumnError, Serializable {

    private double[] innerCols;
    private double[] intraCols;

    private final List<Integer> measures;

    public PixelColumnErrorAggregator(List<Integer> measures) {
        this.measures = measures;
        int length = measures.size();
        innerCols = new double[length];
        intraCols = new double[length];
    }

    public void clear() {
        Arrays.fill(innerCols, 0d);
        Arrays.fill(intraCols, 0d);
    }

    @Override
    public int getId() {
        return 0;
    }

    @Override
    public double[] getInnerColError() {
        return innerCols;
    }

    @Override
    public double[] intraColError() {
        return intraCols;
    }

    @Override
    public void accept(PixelAggregatedDataPoint pixelAggregatedDataPoint) {

    }
}
