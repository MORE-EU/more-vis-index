package eu.more2020.visual.middleware.domain;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import eu.more2020.visual.middleware.cache.MinMaxCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Class that computes the maximum number of pixel errors.
 */
public class MaxErrorEvaluator {
    private static final Logger LOG = LoggerFactory.getLogger(MaxErrorEvaluator.class);

    private final ViewPort viewPort;

    private final List<PixelColumn> pixelColumns;

    private List<TimeInterval> missingRanges;


    public MaxErrorEvaluator(ViewPort viewPort, List<PixelColumn> pixelColumns) {
        this.viewPort = viewPort;
        this.pixelColumns = pixelColumns;
    }

    public List<Integer> computeMaxPixelErrorsPerColumn() {
        List<Integer> maxPixelErrorsPerColumn = new ArrayList<>();
        missingRanges = new ArrayList<>();

        // The stats aggregator for the whole query interval to keep track of the min/max values
        // and determine the y-axis scale.
        StatsAggregator viewPortStatsAggregator = new StatsAggregator();
        pixelColumns.forEach(pixelColumn -> viewPortStatsAggregator.combine(pixelColumn.getStats()));
        LOG.debug("Viewport stats: {}", viewPortStatsAggregator);

        for (int i = 0; i < pixelColumns.size(); i++) {
            PixelColumn currentPixelColumn = pixelColumns.get(i);
            Range<Integer> maxInnerColumnPixelRanges = currentPixelColumn.computeMaxInnerPixelRange(viewPortStatsAggregator);

            if (maxInnerColumnPixelRanges == null) {
                maxPixelErrorsPerColumn.add(null);
                missingRanges.add(currentPixelColumn.getRange()); // add range as missing
                continue;
            }
            RangeSet<Integer> pixelErrorRangeSet = TreeRangeSet.create();

            // Check if there is a previous PixelColumn
            if (i > 0) {
                PixelColumn previousPixelColumn = pixelColumns.get(i - 1);
                if(previousPixelColumn.getStats().getCount() != 0) {
                    Range<Integer> leftMaxFalsePixels = currentPixelColumn.getPixelIdsForLineSegment(previousPixelColumn.getStats().getLastTimestamp(), previousPixelColumn.getStats().getLastValue(),
                            currentPixelColumn.getStats().getFirstTimestamp(), currentPixelColumn.getStats().getFirstValue(), viewPortStatsAggregator);
                    pixelErrorRangeSet.add(leftMaxFalsePixels);
                }
            }
            // Check if there is a next PixelColumn
            if (i < pixelColumns.size() - 1) {
                PixelColumn nextPixelColumn = pixelColumns.get(i + 1);
                if(nextPixelColumn.getStats().getCount() != 0) {
                    Range<Integer> rightMaxFalsePixels = currentPixelColumn.getPixelIdsForLineSegment(currentPixelColumn.getStats().getLastTimestamp(), currentPixelColumn.getStats().getLastValue(),
                            nextPixelColumn.getStats().getFirstTimestamp(), nextPixelColumn.getStats().getFirstValue(), viewPortStatsAggregator);
                    pixelErrorRangeSet.add(rightMaxFalsePixels);
                }
            }
            pixelErrorRangeSet.remove(currentPixelColumn.getActualInnerColumnPixelRange(viewPortStatsAggregator));
            int maxWrongPixels = pixelErrorRangeSet.asRanges().stream()
                    .mapToInt(range -> range.upperEndpoint() - range.lowerEndpoint() + 1)
                    .sum();
            maxPixelErrorsPerColumn.add(maxWrongPixels);
        }
        LOG.debug("{}", maxPixelErrorsPerColumn);
        return maxPixelErrorsPerColumn;
    }

    public List<TimeInterval> getMissingRanges() {
        return missingRanges;
    }


}

