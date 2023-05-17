package eu.more2020.visual.domain;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class TotalErrorEvaluator implements Consumer<PixelAggregatedDataPoint>{

    private final PixelStatsAggregator pixelStats;
    private final List<Integer> measures;
    private final double[] error;
    private int currentPixelColumn;

    public TotalErrorEvaluator(PixelStatsAggregator pixelStats, List<Integer> measures) {
        this.pixelStats = pixelStats;
        this.measures = measures;
        int length = measures.size();
        error = new double[length];
    }

    public void accept(PixelAggregatedDataPoint pixelAggregatedDataPoint) {
        if(pixelAggregatedDataPoint.getCount() != 0){

        }
    }

    private int getMeasureIndex(int measure) {
        return measures.indexOf(measure);
    }

    public double getError(int measure){
        return 0.0;
    }
}

