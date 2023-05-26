package eu.more2020.visual.domain;
import javax.swing.text.View;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class TotalErrorEvaluator implements Consumer<PixelAggregatedDataPoint>{

    private final PixelStatsAggregator pixelStats;
    private final List<Integer> measures;
    private final double[] error;
    private ViewPort viewPort;
    private int[] missingMinId;
    private int[] missingMaxId;
    private int[] lastPixelId;


    public TotalErrorEvaluator(PixelStatsAggregator pixelStats, List<Integer> measures, ViewPort viewPort) {
        this.pixelStats = pixelStats;
        this.measures = measures;
        this.viewPort = viewPort;
        int length = measures.size();
        error = new double[length];
        lastPixelId = new int[length];
        missingMinId = new int[length];
        missingMaxId = new int[length];
        Arrays.fill(missingMinId, -1);
        Arrays.fill(missingMaxId, -1);
        Arrays.fill(lastPixelId, -1);
    }

    public void accept(PixelAggregatedDataPoint pixelAggregatedDataPoint) {
        if(pixelAggregatedDataPoint.getCount() != 0){
            PixelStatsAggregator pixelStatsAggregator = (PixelStatsAggregator) (pixelAggregatedDataPoint.getStats());
            int i = 0;
            for (int m : measures) {
                int firstPixelId = pixelStatsAggregator.getFirstPixelId(m);
                if(lastPixelId[i] != -1) {
                    int minPixelId = pixelStatsAggregator.getMinPixelId(m);
                    int maxPixelId = pixelStatsAggregator.getMaxPixelId(m);
                    // Inner Column
                    int trueMinPixelId = pixelStatsAggregator.getTrueMinPixelId(m);
                    int trueMaxPixelId = pixelStatsAggregator.getTrueMaxPixelId(m);
                    error[i] += minPixelId - trueMinPixelId;
                    error[i] += trueMaxPixelId - maxPixelId;
                    // Intra Column False
                    int[] coords = getLineSegment(i, lastPixelId[i], firstPixelId);
                    int startPixelId = coords[0];
                    int endPixelId = coords[1];
                    if(!(startPixelId >= minPixelId && startPixelId <= maxPixelId &&
                            endPixelId >= minPixelId && endPixelId <= maxPixelId)){
                        error[i] += (startPixelId - minPixelId) + (maxPixelId - endPixelId);
                    };
                    // Intra Column Missing
                    if(!(minPixelId < missingMinId[i] && missingMinId[i] < maxPixelId && missingMaxId[i] <= maxPixelId)){
                        error[i] += missingMaxId[i] - missingMinId[i];
                    }
                }
                lastPixelId[i] = pixelStatsAggregator.getLastPixelId(m);
                i ++;
            }
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
        coords[0] = currentStartPixelId;
        coords[1] = currentEndPixelId;
        return coords;
    }

    private int getMeasureIndex(int m) {
        return measures.indexOf(m);
    }

    public double getError(int m){
        return error[getMeasureIndex(m)] / (viewPort.getHeight() * viewPort.getWidth());
    }

    public int getPixelId(int m, double value){
        double range = Math.abs(pixelStats.getGlobalStats().getMaxValue(m)) + Math.abs(pixelStats.getGlobalStats().getMinValue(m));
        double bin_size = range / viewPort.getHeight();
        return (int) ((Math.abs(value) / bin_size));
    }

    public void acceptPartial(AggregatedDataPoint pixelAggregatedDataPoint) {
        if (pixelAggregatedDataPoint.getCount() != 0) {
            Stats statsAggregator = pixelAggregatedDataPoint.getStats();
            int i = 0;
            for (int m : measures) {
                missingMinId[i] = getPixelId(m, statsAggregator.getMinValue(m));
                missingMaxId[i] =  getPixelId(m, statsAggregator.getMaxValue(m));
                i ++;
            }
        }
    }
}

