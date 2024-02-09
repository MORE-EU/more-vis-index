package eu.more2020.visual.middleware.cache;

import eu.more2020.visual.middleware.domain.*;
import eu.more2020.visual.middleware.domain.Query.Query;
import eu.more2020.visual.middleware.util.DateTimeUtil;
import org.apache.arrow.flatbuf.Int;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class ErrorCalculator {
    private static final Logger LOG = LoggerFactory.getLogger(MinMaxCache.class);

    private MaxErrorEvaluator maxErrorEvaluator;
    private boolean hasError = true;
    private long pixelColumnInterval;
    private double error;

    public double calculateTotalError(List<PixelColumn> pixelColumns, ViewPort viewPort, long pixelColumnInterval, double accuracy) {
        // Calculate errors using processed data
        maxErrorEvaluator = new MaxErrorEvaluator(viewPort, pixelColumns);
        this.pixelColumnInterval = pixelColumnInterval;
        List<Double> pixelColumnErrors = maxErrorEvaluator.computeMaxPixelErrorsPerColumn();
        // Find the part of the query interval that is not covered by the spans in the interval tree.
        int validColumns = 0;
        error = 0.0;
        for (Double pixelColumnError : pixelColumnErrors) {
            if(pixelColumnError != null) {
                validColumns += 1;
                error += pixelColumnError;
            }
        }
        LOG.debug("Valid columns: {}", validColumns);
        error /= validColumns;
//        error = error / (viewPort.getHeight() * validColumns);
        hasError = error > 1 - accuracy;
        return error;
    }

    public List<TimeInterval> getMissingIntervals() {
        List<TimeInterval> missingIntervals = maxErrorEvaluator.getMissingRanges();
        missingIntervals = DateTimeUtil.groupIntervals(pixelColumnInterval, missingIntervals);
        LOG.info("Unable to Determine Errors: " + missingIntervals);
        return missingIntervals;
    }

    public boolean hasError(){
        return hasError;
    }
}
