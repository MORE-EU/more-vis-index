package eu.more2020.visual.middleware.domain;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import eu.more2020.visual.middleware.cache.MinMaxCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class that computes the maximum number of pixel errors.
 */
public class MaxErrorEvaluator {
    private static final Logger LOG = LoggerFactory.getLogger(MinMaxCache.class);

    private final List<Integer> measures;
    private final ViewPort viewPort;

    private final List<PixelColumn> pixelColumns;

    private List<TimeInterval> missingRanges;

    private List<List<TimeInterval>> missingRangesPerMeasure;



    public MaxErrorEvaluator(List<Integer> measures, ViewPort viewPort, List<PixelColumn> pixelColumns) {
        this.measures = measures;
        this.viewPort = viewPort;
        this.pixelColumns = pixelColumns;
    }

    public List<List<Integer>> computeMaxPixelErrorsPerColumnAndMeasure() {
        List<List<Integer>> maxPixelErrorsPerColumnAndMeasure = new ArrayList<>();
        missingRanges = new ArrayList<>();
        missingRangesPerMeasure = new ArrayList<>();
        while (missingRangesPerMeasure.size() < measures.size()) {
            missingRangesPerMeasure.add(new ArrayList<>());
        }
        // The stats aggregator for the whole query interval to keep track of the min/max values
        // and determine the y-axis scale.
        StatsAggregator viewPortStatsAggregator = new StatsAggregator(measures);
        pixelColumns.forEach(pixelColumn -> viewPortStatsAggregator.combine(pixelColumn.getStats()));

        for (int i = 0; i < pixelColumns.size(); i++) {
            PixelColumn currentPixelColumn = pixelColumns.get(i);
            List<Range<Integer>> maxInnerColumnPixelRanges = currentPixelColumn.computeMaxInnerPixelRange(viewPortStatsAggregator);
            if (maxInnerColumnPixelRanges == null) {
                maxPixelErrorsPerColumnAndMeasure.add(null);
                for (int measureIdx = 0; measureIdx < measures.size(); measureIdx++) {
                    int measure = measures.get(measureIdx);
                    missingRangesPerMeasure.get(measureIdx).add(currentPixelColumn.getRange());
                }
                missingRanges.add(currentPixelColumn.getRange());
                continue;
            }

            List<Integer> maxPixelErrorsPerMeasure = new ArrayList<>();
            for (int measureIdx = 0; measureIdx < measures.size(); measureIdx++) {
                int measure = measures.get(measureIdx);
                RangeSet<Integer> pixelErrorRangeSet = TreeRangeSet.create();

                // If error is null then add to list
                if(maxInnerColumnPixelRanges.get(measureIdx) ==  null){
                    maxPixelErrorsPerMeasure.add(null);
                    missingRangesPerMeasure.get(measureIdx).add(currentPixelColumn.getRange());
                    continue;
                }

                pixelErrorRangeSet.add(maxInnerColumnPixelRanges.get(measureIdx));

                // Check if there is a previous PixelColumn
                if (i > 0) {
                    PixelColumn previousPixelColumn = pixelColumns.get(i - 1);
                    if(previousPixelColumn.getStats().getCount() != 0) {
                        Range<Integer> leftMaxFalsePixels = currentPixelColumn.getPixelIdsForLineSegment(measure, previousPixelColumn.getStats().getLastTimestamp(measure), previousPixelColumn.getStats().getLastValue(measure),
                                currentPixelColumn.getStats().getFirstTimestamp(measure), currentPixelColumn.getStats().getFirstValue(measure), viewPortStatsAggregator);
                        pixelErrorRangeSet.add(leftMaxFalsePixels);
                    }
                }
                // Check if there is a next PixelColumn
                if (i < pixelColumns.size() - 1) {
                    PixelColumn nextPixelColumn = pixelColumns.get(i + 1);
                    if(nextPixelColumn.getStats().getCount() != 0) {
                        Range<Integer> rightMaxFalsePixels = currentPixelColumn.getPixelIdsForLineSegment(measure, currentPixelColumn.getStats().getLastTimestamp(measure), currentPixelColumn.getStats().getLastValue(measure),
                                nextPixelColumn.getStats().getFirstTimestamp(measure), nextPixelColumn.getStats().getFirstValue(measure), viewPortStatsAggregator);
                        pixelErrorRangeSet.add(rightMaxFalsePixels);
                    }
                }
                pixelErrorRangeSet.remove(currentPixelColumn.getActualInnerColumnPixelRange(measure, viewPortStatsAggregator));

                int maxWrongPixelsForMeasure = pixelErrorRangeSet.asRanges().stream()
                        .mapToInt(range -> range.upperEndpoint() - range.lowerEndpoint() + 1)
                        .sum();
                maxPixelErrorsPerMeasure.add(maxWrongPixelsForMeasure);
            }
            maxPixelErrorsPerColumnAndMeasure.add(maxPixelErrorsPerMeasure);
        }
        LOG.debug("Wrong pixels: {}", missingRangesPerMeasure);
        LOG.debug("{}", maxPixelErrorsPerColumnAndMeasure);
        return maxPixelErrorsPerColumnAndMeasure;
    }

    public List<TimeInterval> getMissingRanges() {
        return missingRanges;
    }

    public List<List<TimeInterval>> getMissingRangesPerMeasure() {
        return missingRangesPerMeasure;
    }
}

