//package eu.more2020.visual.domain;
//
//import java.io.Serializable;
//import java.time.ZonedDateTime;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//import java.util.function.Consumer;
//
//public class TotalErrorEvaluator implements Consumer<PixelAggregatedDataPoint>{
//
//    private final List<PixelColumnErrorAggregator> errorAggregators;
//    private final StatsAggregator stats;
//    private final List<Integer> measures;
//    private final ViewPort viewport;
//    private final AggregateInterval interval;
//    private final double[] error;
//
//    private int currentPixelColumn;
//    private boolean finishedIt = false;
//
//    public TotalErrorEvaluator(StatsAggregator stats, List<Integer> measures, ViewPort viewport, AggregateInterval interval) {
//        this.errorAggregators = new ArrayList<>();
//        this.stats = stats;
//        this.measures = measures;
//        this.viewport = viewport;
//        this.interval = interval;
//        int length = measures.size();
//        error = new double[length];
//    }
//
//    public void accept(PixelAggregatedDataPoint pixelAggregatedDataPoint) {
//        // if overlapping then accept both in this and the next
//        errorAggregators.get(currentPixelColumn).accept(pixelAggregatedDataPoint);
//        if(pixelAggregatedDataPoint.isOverlapping()) errorAggregators.get(currentPixelColumn + 1).accept(pixelAggregatedDataPoint);
//    }
//
//    public void add(ZonedDateTime pixel){
//        this.errorAggregators.add(new PixelColumnErrorAggregator(measures, stats, interval, pixel, viewport.getHeight()));
//        currentPixelColumn++;
//    }
//
//    public void initialize(ZonedDateTime startPixel){
//        ZonedDateTime nextPixel = startPixel.plus(interval.getInterval(), interval.getChronoUnit());
//        add(startPixel);
//        add(nextPixel);
//        currentPixelColumn = 0;
//    }
//
//    public void update(){
//        if(finishedIt) return;
//        update(currentPixelColumn, currentPixelColumn - 1);
//        if(currentPixelColumn == viewport.getWidth() - 1) finishedIt = true;
//    }
//
//    public void update(int update){
//        update(currentPixelColumn, update);
//    }
//
//    public void update(int current, int update){
//        if (currentPixelColumn != 0) // update prev error for intra col
//            errorAggregators.get(update).accept(errorAggregators.get(current));
//    }
//
//    private int getMeasureIndex(int measure) {
//        return measures.indexOf(measure);
//    }
//
//    public double getError(int measure){
//        return errorAggregators.get(currentPixelColumn).getError(getMeasureIndex(measure));
//    }
//
//    public class PixelColumnErrorAggregator implements Consumer<PixelAggregatedDataPoint>, PixelColumnError, Serializable {
//
//        private final int[] minId;
//        private final int[] maxId;
//        private final int[] trueMinId;
//        private final int[] trueMaxId;
//
//        private final Stats stats;
//        private final List<Integer> measures;
//        private final AggregateInterval interval;
//        private final ZonedDateTime pixelColumn;
//        private final int height;
//
//        public PixelColumnErrorAggregator(List<Integer> measures, Stats stats, AggregateInterval interval, ZonedDateTime pixelColumn, int height) {
//            this.measures = measures;
//            this.height = height;
//            this.stats = stats;
//            this.interval = interval;
//            this.pixelColumn = pixelColumn;
//            int length = measures.size();
//
//            minId = new int[length];
//            maxId = new int[length];
//            trueMinId = new int[length];
//            trueMaxId = new int[length];
//            clear();
//        }
//
//        public void clear() {
//            Arrays.fill(minId, -1);
//            Arrays.fill(maxId, -1);
//            Arrays.fill(trueMinId, -1);
//            Arrays.fill(trueMaxId, -1);
//        }
//
//        @Override
//        public int getId() {
//            return 0;
//        }
//
//        @Override
//        public double getError(int i) {
//            return error[i];
//        }
//
//
//        @Override
//        public void accept(PixelAggregatedDataPoint pixelAggregatedDataPoint) {
//            int i = 0;
//            for (int m : measures) {
//                int min = getPixelId(m, pixelAggregatedDataPoint.getStats().getMinValue(m));
//                int max = getPixelId(m, pixelAggregatedDataPoint.getStats().getMaxValue(m));
//                long minTimestamp = pixelAggregatedDataPoint.getStats().getMinTimestamp(m);
//                long maxTimestamp = pixelAggregatedDataPoint.getStats().getMaxTimestamp(m);
//                // if its fully overlapping initialize
//                boolean containsMin = true;
//                boolean containsMax = true;
//                if(pixelAggregatedDataPoint.isOverlapping()) { // check if min max falls in pixel column
//                    containsMin = minTimestamp < (pixelColumn.plus(interval.getInterval(), interval.getChronoUnit()).toInstant().toEpochMilli()) &&
//                            minTimestamp >= (pixelColumn.toInstant().toEpochMilli());
//                    containsMax = maxTimestamp < (pixelColumn.plus(interval.getInterval(), interval.getChronoUnit()).toInstant().toEpochMilli()) &&
//                            maxTimestamp >= (pixelColumn.toInstant().toEpochMilli());
//                    if(min >= minId[i] && max <= maxId[i]) continue;
//                }
//                minId[i] = containsMin ? Math.min(min, minId[i]) : minId[i];
//                maxId[i] = containsMax ? Math.max(max, maxId[i]) : maxId[i];
//                trueMinId[i] = Math.min(minId[i], min);
//                trueMaxId[i] =  Math.max(maxId[i], max);
//                i++;
//            }
//        }
//
//        public void accept(PixelColumnErrorAggregator pixelColumnErrorAggregator) {
//            for (int i = 0; i < error.length; i++) {
//                trueMinId[i] = Math.min(trueMinId[i], pixelColumnErrorAggregator.getMinId(i));
//                trueMaxId[i] = Math.max(trueMaxId[i], pixelColumnErrorAggregator.getMaxId(i));
//                pixelColumnErrorAggregator.setTrueMinId(Math.min(minId[i], pixelColumnErrorAggregator.getTrueMinId(i)), i);
//                pixelColumnErrorAggregator.setTrueMaxId(Math.max(maxId[i], pixelColumnErrorAggregator.getTrueMaxId(i)), i);
//                double currError =  Math.abs(minId[i] - trueMinId[i]) + Math.abs(trueMaxId[i] - maxId[i]);
//                error[i] += (currError / (double)(viewport.getWidth() * viewport.getHeight()));
//            }
//        }
//
//        public int getPixelId(int m, double value){
//            double range = Math.abs(stats.getMaxValue(m)) + Math.abs(stats.getMinValue(m));
//            double bin_size = range / height;
//            return (int) ((Math.abs(value) / bin_size) + Math.abs(stats.getMinValue(m)));
//        }
//
//        public void setTrueMinId(int trueMinId, int id) {
//            this.trueMinId[id] = trueMinId;
//        }
//
//        public void setTrueMaxId(int trueMaxId, int id) {
//            this.trueMaxId[id] = trueMaxId;
//        }
//
//        public int getMinId(int id) {
//            return minId[id];
//        }
//
//        public int getTrueMinId(int id) {
//            return trueMinId[id];
//        }
//
//        public int getMaxId(int id) {
//            return maxId[id];
//        }
//
//        public int getTrueMaxId(int id) {
//            return trueMaxId[id];
//        }
//
//
//    }
//}
//
