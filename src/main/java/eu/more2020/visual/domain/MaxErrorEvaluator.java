package eu.more2020.visual.domain;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Class that computes the maximum number of pixel errors.
 */
public class MaxErrorEvaluator {

    private final List<Integer> measures;
    private final ViewPort viewPort;

    private final List<PixelColumn> pixelColumns;


    public MaxErrorEvaluator(List<Integer> measures, ViewPort viewPort, List<PixelColumn> pixelColumns) {
        this.measures = measures;
        this.viewPort = viewPort;
        this.pixelColumns = pixelColumns;
    }

/*    public double[] computeMaxPixelErrorsPerColumn() {
        int[] maxWrongPixelsByMeasure = new int[measures.size()];
        // The stats aggregator for the whole query interval to keep track of the min/max values
        // and determine the y-axis scale.
        StatsAggregator viewPortStatsAggregator = new StatsAggregator(measures);
        pixelColumns.forEach(pixelColumn -> viewPortStatsAggregator.combine(pixelColumn.getStats()));

        // Determine for each pixel column its pixel range that is intersected by the intra-column line segment.
        // This range is the max range of false pixels for this pixel column.

        for (int i = 0; i < pixelColumns.size(); i++) {
            PixelColumn currentPixelColumn = pixelColumns.get(i);
            List<Range<Integer>> maxInnerColumnPixelRanges = currentPixelColumn.computeMaxInnerPixelRange(viewPortStatsAggregator);
            for (int measureIdx = 0; measureIdx < measures.size(); measureIdx++) {
                int measure = measures.get(measureIdx);

                RangeSet<Integer> pixelErrorRangeSet = TreeRangeSet.create();

                pixelErrorRangeSet.add(maxInnerColumnPixelRanges.get(measureIdx));

                // Check if there is a previous PixelColumn
                if (i > 0) {
                    PixelColumn previousPixelColumn = pixelColumns.get(i - 1);
                    Range<Integer> leftMaxFalsePixels = currentPixelColumn.getPixelIdsForLineSegment(measure, previousPixelColumn.getStats().getLastTimestamp(measure), previousPixelColumn.getStats().getLastValue(measure),
                            currentPixelColumn.getStats().getFirstTimestamp(measure), currentPixelColumn.getStats().getFirstValue(measure), viewPortStatsAggregator);
                    pixelErrorRangeSet.add(leftMaxFalsePixels);
                }
                // Check if there is a next PixelColumn
                if (i < pixelColumns.size() - 1) {
                    PixelColumn nextPixelColumn = pixelColumns.get(i + 1);
                    Range<Integer> rightMaxFalsePixels = currentPixelColumn.getPixelIdsForLineSegment(measure, currentPixelColumn.getStats().getLastTimestamp(measure), currentPixelColumn.getStats().getLastValue(measure),
                            currentPixelColumn.getStats().getFirstTimestamp(measure), currentPixelColumn.getStats().getFirstValue(measure), viewPortStatsAggregator);
                    pixelErrorRangeSet.add(rightMaxFalsePixels);
                }
                pixelErrorRangeSet.remove(currentPixelColumn.getActualInnerColumnPixelRange(measure, viewPortStatsAggregator));

                maxWrongPixelsByMeasure[i] += pixelErrorRangeSet.asRanges().stream()
                        .mapToInt(range -> range.upperEndpoint() - range.lowerEndpoint() + 1)
                        .sum();
            }
        }
        return Arrays.stream(maxWrongPixelsByMeasure).mapToDouble(maxPixels -> maxPixels / (viewPort.getHeight() * viewPort.getWidth())).toArray();
    }*/


    public List<List<Integer>> computeMaxPixelErrorsPerColumnAndMeasure() {
        List<List<Integer>> maxPixelErrorsPerColumnAndMeasure = new ArrayList<>();

        // The stats aggregator for the whole query interval to keep track of the min/max values
        // and determine the y-axis scale.
        StatsAggregator viewPortStatsAggregator = new StatsAggregator(measures);
        pixelColumns.forEach(pixelColumn -> viewPortStatsAggregator.combine(pixelColumn.getStats()));

        for (int i = 0; i < pixelColumns.size(); i++) {
            PixelColumn currentPixelColumn = pixelColumns.get(i);
            List<Range<Integer>> maxInnerColumnPixelRanges = currentPixelColumn.computeMaxInnerPixelRange(viewPortStatsAggregator);

            if (maxInnerColumnPixelRanges == null) {
                maxPixelErrorsPerColumnAndMeasure.add(null);
                continue;
            }

            List<Integer> maxPixelErrorsPerMeasure = new ArrayList<>();
            for (int measureIdx = 0; measureIdx < measures.size(); measureIdx++) {
                int measure = measures.get(measureIdx);

                RangeSet<Integer> pixelErrorRangeSet = TreeRangeSet.create();

                pixelErrorRangeSet.add(maxInnerColumnPixelRanges.get(measureIdx));

                // Check if there is a previous PixelColumn
                if (i > 0) {
                    PixelColumn previousPixelColumn = pixelColumns.get(i - 1);
                    Range<Integer> leftMaxFalsePixels = currentPixelColumn.getPixelIdsForLineSegment(measure, previousPixelColumn.getStats().getLastTimestamp(measure), previousPixelColumn.getStats().getLastValue(measure),
                            currentPixelColumn.getStats().getFirstTimestamp(measure), currentPixelColumn.getStats().getFirstValue(measure), viewPortStatsAggregator);
                    pixelErrorRangeSet.add(leftMaxFalsePixels);
                }
                // Check if there is a next PixelColumn
                if (i < pixelColumns.size() - 1) {
                    PixelColumn nextPixelColumn = pixelColumns.get(i + 1);
                    Range<Integer> rightMaxFalsePixels = currentPixelColumn.getPixelIdsForLineSegment(measure, currentPixelColumn.getStats().getLastTimestamp(measure), currentPixelColumn.getStats().getLastValue(measure),
                            currentPixelColumn.getStats().getFirstTimestamp(measure), currentPixelColumn.getStats().getFirstValue(measure), viewPortStatsAggregator);
                    pixelErrorRangeSet.add(rightMaxFalsePixels);
                }
                pixelErrorRangeSet.remove(currentPixelColumn.getActualInnerColumnPixelRange(measure, viewPortStatsAggregator));

                int maxWrongPixelsForMeasure = pixelErrorRangeSet.asRanges().stream()
                        .mapToInt(range -> range.upperEndpoint() - range.lowerEndpoint() + 1)
                        .sum();
                maxPixelErrorsPerMeasure.add(maxWrongPixelsForMeasure);
            }

            maxPixelErrorsPerColumnAndMeasure.add(maxPixelErrorsPerMeasure);
        }

        return maxPixelErrorsPerColumnAndMeasure;
    }


}

