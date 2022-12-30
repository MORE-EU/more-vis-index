package eu.more2020.visual.experiments.util;


import com.google.common.collect.Range;

import java.util.ArrayList;
import java.util.List;

public class RandomRangeQueryGenerator {

    private Range<Float> valueRange;
    private RangeGenerator rangeGenerator;


    public RandomRangeQueryGenerator(Range<Float> valueRange) {
        this.valueRange = valueRange;
        this.rangeGenerator = new RangeGenerator(valueRange);
    }


//    public Rectangle generate(float selectivity) {
//        Range<Float>[] ranges = this.rangeGenerator.getEqualSizedRanges(2, selectivity);
//        return new Rectangle(ranges[0], ranges[1]);
//    }
//
//
//    public List<Rectangle> generate(int count, float selectivity) {
//        List<Rectangle> rangeQueries = new ArrayList<>();
//        for (int i = 0; i < count; i++) {
//            rangeQueries.add(this.generate(selectivity));
//        }
//        return rangeQueries;
//    }
}
