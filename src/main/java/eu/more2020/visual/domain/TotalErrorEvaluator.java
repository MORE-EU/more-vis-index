package eu.more2020.visual.domain;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class TotalErrorEvaluator implements Consumer<PixelAggregatedDataPoint>{

    private final PixelStatsAggregator pixelStats;
    private final List<Integer> measures;
    private final double[] error;
    private int[] lastPixelId;


    public TotalErrorEvaluator(PixelStatsAggregator pixelStats, List<Integer> measures) {
        this.pixelStats = pixelStats;
        this.measures = measures;
        int length = measures.size();
        error = new double[length];
        lastPixelId = new int[length];
        Arrays.fill(lastPixelId, -1);
    }

    public void accept(PixelAggregatedDataPoint pixelAggregatedDataPoint) {
        if(pixelAggregatedDataPoint.getCount() != 0){
            PixelStatsAggregator pixelStatsAggregator = (PixelStatsAggregator) (pixelAggregatedDataPoint.getStats());
            int i = 0;
            for (int m : measures) {
                int firstPixelId = pixelStatsAggregator.getFirstPixelId(m);
                if(lastPixelId[i] != -1) {
                    getCoordinates(lastPixelId[i], firstPixelId);
                }
                lastPixelId[i] = pixelStatsAggregator.getLastPixelId(m);
                i ++;
            }
        }
    }

    private void getCoordinates(int start_y, int end_y) {
        int sy = start_y < end_y ? 1 : -1;
        int dy = Math.abs(end_y - start_y);
        int sum = Math.abs(dy) + 1;
        int half = (int) Math.floor((dy) / 2.0);
        int prevMinPixelId = start_y;
        int prevMaxPixelId = start_y + (sy * half);
        int minPixelId = sum != 1 ? start_y + (sy * half) + sy : start_y + (sy * half);
        int maxPixelId = end_y;
    }

    private void getLine(int start_y, int end_y) {
        int start_x = 0;
        int end_x = 1;
        int sx = start_x < end_x ?  1 : -1;
        int sy = start_y < end_y ? 1 : -1;
        int dy = Math.abs(end_y - start_y);
        int dx = Math.abs(end_x - start_x);
        int err = dx - dy;
        List<PixelCoordinates> pixelCoordinates = new ArrayList<>();
        while (start_x != end_x || start_y != end_y){
            pixelCoordinates.add(new PixelCoordinates(start_x, start_y));
            int e2 = 2 * err;
            if (e2 > -dy) {
                err -= dy;
                start_x += sx;
            }
            if (e2 < dx) {
                err += dx;
                start_y += sy;
            }
       }
        pixelCoordinates.add(new PixelCoordinates(end_x, end_y));
        System.out.println(pixelCoordinates);
    }

    private int getMeasureIndex(int measure) {
        return measures.indexOf(measure);
    }

    public double getError(int measure){
        return 0.0;
    }

    private class PixelCoordinates {

        private int x;
        private int y;

        public PixelCoordinates(int x, int y) {
            this.x = x;
            this.y = y;
        }

        @Override
        public String toString() {
            return "(" + x + ", " + y + ")";
        }
    }
}

