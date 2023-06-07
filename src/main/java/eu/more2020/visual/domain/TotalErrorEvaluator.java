package eu.more2020.visual.domain;
import com.google.common.collect.*;

import java.util.ArrayList;
import java.util.List;

public class TotalErrorEvaluator {

    private final PixelStatsAggregator pixelStats;
    private final List<Integer> measures;
    private final double[] error;
    private ViewPort viewPort;


    public TotalErrorEvaluator(PixelStatsAggregator pixelStats, List<Integer> measures, ViewPort viewPort) {
        this.pixelStats = pixelStats;
        this.measures = measures;
        this.viewPort = viewPort;
        int length = measures.size();
        error = new double[length];
    }

    public void accept(PixelAggregatedDataPoint pixelAggregatedDataPoint,
                       PixelColumn current, PixelColumn previous, PixelColumn next) {
        /*if(pixelAggregatedDataPoint.getCount() != 0){
            PixelStatsAggregator pixelStatsAggregator = (PixelStatsAggregator) (pixelAggregatedDataPoint.getStats());
            int i = 0;
            for (int m : measures) {
                int firstPixelId = pixelStatsAggregator.getFirstPixelId(m);
                if(previous != null) {
                    List<Range<Integer>> falseData = new ArrayList<>();
                    int minPixelId = pixelStatsAggregator.getMinPixelId(m);
                    int maxPixelId = pixelStatsAggregator.getMaxPixelId(m);
                    Range<Integer> trueData = Range.closed(minPixelId, maxPixelId);
                    // Inner Column
                    int trueMinPixelId = pixelStatsAggregator.getTrueMinPixelId(m);
                    int trueMaxPixelId = pixelStatsAggregator.getTrueMaxPixelId(m);
                    falseData.add(Range.closed(trueMinPixelId, minPixelId));
                    falseData.add(Range.closed(maxPixelId, trueMaxPixelId));
                    // Intra Column False
                    int[] coords = getLineSegment(i, previous.getStats().getLastPixelId(m), firstPixelId);
                    int startPixelId = coords[0];
                    int endPixelId = coords[1];
                    falseData.add(Range.closed(startPixelId, endPixelId));
                    // Intra Column Missing
                    Stats prevLastStats = previous.getAggregatedDataPoints().get(previous.getAggregatedDataPoints().size() - 1).getStats();
                    Range<Integer> missingData = Range.closed(0, 0);
                    computeError(falseData, missingData, trueData, i);
                }
                i ++;
            }
        }*/
    }

    private void computeError(List<Range<Integer>> falseData,
                              Range<Integer> missingData,
                              Range<Integer> trueData, int i) {
        falseData.add(missingData);
        ImmutableRangeSet<Integer> rangeSet =
                ImmutableRangeSet.unionOf(falseData);
        DiscreteDomain<Integer> domain = DiscreteDomain.integers();
        for (Range r : rangeSet.asRanges()){
            if(!r.isEmpty())
                error[i] += ContiguousSet.create(r, domain).size();
        }
    }

    private int[] getLineSegment(int i, int start_y, int end_y) {

        int[] coords = new int[]{0, 0};
        int sy = start_y < end_y ? 1 : -1;
        int dy = Math.abs(end_y - start_y);
        int sum = Math.abs(dy) + 1;
        int half = (int) Math.floor((dy) / 2.0);
        int prevStartPixelId = start_y;
        int prevEndPixelId = start_y + (sy * half);
        int currentStartPixelId = sum != 1 ? prevStartPixelId + (sy * half) + sy : prevEndPixelId + (sy * half);
        int currentEndPixelId = end_y;

        if(currentStartPixelId < currentEndPixelId) {
            coords[0] = currentStartPixelId;
            coords[1] = currentEndPixelId;
        } else {
            coords[1] = currentStartPixelId;
            coords[0] = currentEndPixelId;
        }

        return coords;
    }

    private int getMeasureIndex(int m) {
        return measures.indexOf(m);
    }

    public double getError(int m){
        // Create the superset of missing + false
        return error[getMeasureIndex(m)] / (viewPort.getHeight() * viewPort.getWidth());
    }

    public int getPixelId(int m, double value){
        double range = Math.abs(pixelStats.getGlobalStats().getMaxValue(m)) + Math.abs(pixelStats.getGlobalStats().getMinValue(m));
        double bin_size = range / viewPort.getHeight();
        return (int) ((Math.abs(value) / bin_size));
    }

}

